package main

import (
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"net"
	"time"
)

// Response is a simple encapsulation for dispatcher response
type Response struct {
	msg string
	err error
}

// DispNode represents basic information of a dispatcher node
type DispNode struct {
	dispid   string
	bindAddr string
}

// DispConn manages a TCP connection to a specific dispatcher
type DispConn struct {
	dispid   string
	conn     net.Conn
	sender   chan []byte
	receiver chan *Response
}

func buildDispConn(dnode *DispNode) (net.Conn, error) {
	addr, err := net.ResolveTCPAddr("tcp", dnode.bindAddr)
	if err != nil {
		return nil, err
	}
	return net.DialTCP("tcp", nil, addr)
}

func dispMsgSender(dnode *DispNode, msg []byte) error {
	dispconn, err := getDispConn(dnode)
	if err != nil {
		return err
	}
	// TODO: error handling. e.g. broken connection, write failed etc.
	// FIXME benchmark shows great latency using channel way: disp.sender <- msg
	go func() {
		dispconn.conn.Write(msg)
	}()

	return nil
}

func getDispConn(dnode *DispNode) (*DispConn, error) {
	dispconn, ok := DispConns[dnode.dispid]
	if !ok {
		conn, err := buildDispConn(dnode)
		if err != nil {
			return nil, err
		}

		dispconn = &DispConn{
			dispid:   dnode.dispid,
			conn:     conn,
			sender:   make(chan []byte),
			receiver: make(chan *Response),
		}
		DispConns[dnode.dispid] = dispconn
		dispconn.lifeCycle()
	}
	return dispconn, nil
}

func (dispconn *DispConn) heartbeat() {
	rawmsg := &BasicMsg{
		cmdType: dmq.MDMsgCmdHeartbeat,
		bodyLen: 0,
		extra:   dmq.MDMsgExtraNone,
		items:   make(map[uint8]string),
	}
	bmsg := binaryMsgEncode(rawmsg)
	dispconn.sender <- bmsg
}

func (dispconn *DispConn) heartbeatRoutine(interval int) {
	ticker := time.NewTicker(time.Second * time.Duration(interval))
	for {
		<-ticker.C
		dispconn.heartbeat()
	}
}

func (dispconn *DispConn) lifeCycle() {
	go dispconn.heartbeatRoutine(hbIntervalToDisp)

	go func() {
		for {
			select {
			case msg := <-dispconn.sender:
				dispconn.conn.Write(msg)
			case resp := <-dispconn.receiver:
				if resp.err != nil {
					log.Debug("dispconn to %s receive err: %v", dispconn.dispid, resp.err)

					// ignore TCP connection close error
					dispconn.conn.Close()
					delete(DispConns, dispconn.dispid)
					return
				}
				log.Debug("receive msg: '%s' from dispconn %s", resp.msg, dispconn.dispid)
			}
		}
	}()

	go func() {
		for {
			b := make([]byte, 2048)
			_, err := dispconn.conn.Read(b)
			dispconn.receiver <- &Response{msg: string(b), err: err}
			if err != nil {
				return
			}
		}
	}()
}
