package main

import (
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"io"
	"net"
	"time"
)

// DispConn struct
type DispConn struct {
	connid string
	dispid string
	conn   net.Conn
	sender chan []byte
	stop   chan bool
}

func dispMsgSender(connid string, msg []byte) error {
	dpconn, err := getDispConn(connid)
	if err != nil {
		return err
	}
	// FIXME: benchmark shows great latency using channel way: dpconn.sender <- msg
	go func() {
		dpconn.conn.Write(msg)
	}()

	return nil
}

// Get a DispConn struct that contains a TCP connection to a dispatcher node,
// which is connecting with the connector of connid.
func getDispConn(connid string) (*DispConn, error) {
	dispConn, ok := DispConns[connid]
	if !ok {
		info, err := getConnRelatedDispInfo(connid, EtcdCliPool)
		if err != nil {
			return nil, err
		}
		addr, err := net.ResolveTCPAddr("tcp", info["addr"])
		if err != nil {
			return nil, err
		}
		conn, err := net.DialTCP("tcp", nil, addr)
		if err != nil {
			return nil, err
		}

		dispConn = &DispConn{
			connid: connid,
			dispid: info["dispid"],
			conn:   conn,
			sender: make(chan []byte),
			stop:   make(chan bool),
		}

		DispConns[connid] = dispConn
		dispConn.lifeCycle()
	}
	return dispConn, nil
}

func (dpconn *DispConn) heartbeat() {
	rawmsg := &BasicMsg{
		cmdType: dmq.MDMsgCmdHeartbeat,
		bodyLen: 0,
		extra:   dmq.MDMsgExtraNone,
		items:   make(map[uint8]string),
	}
	bmsg := binaryMsgEncode(rawmsg)
	dpconn.sender <- bmsg
}

func (dpconn *DispConn) heartbeatRoutine(interval int) {
	ticker := time.NewTicker(time.Second * time.Duration(interval))
	for {
		<-ticker.C
		dpconn.heartbeat()
	}
}

func (dpconn *DispConn) lifeCycle() {
	go dpconn.heartbeatRoutine(hbIntervalToDisp)

	go func() {
		buf := make([]byte, 64)
		for {
			if _, err := dpconn.conn.Read(buf); err != nil {
				if err == io.EOF {
					log.Info("addr: %s close connection",
						dpconn.conn.LocalAddr().String())
				} else {
					log.Error("addr: %s read error: %v",
						dpconn.conn.LocalAddr().String(), err)
				}
				dpconn.stop <- true
				break
			}
		}
	}()

	go func() {
		for {
			select {
			case msg := <-dpconn.sender:
				dpconn.conn.Write(msg)
			case <-dpconn.stop:
				dpconn.conn.Close()
				delete(DispConns, dpconn.connid)
				return
			}
		}
	}()
}
