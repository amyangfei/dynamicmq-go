package main

import (
	"bytes"
	"fmt"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"net"
	"sort"
	"time"
)

// physical node in datanode cluster
type Pnode struct {
	id       string
	bindAddr string
	vnum     int // vnode number
}

// virtual node in datanode cluster
type Vnode struct {
	id []byte // virtual node id
	pn *Pnode
}

type RTable struct {
	vns []*Vnode
}

type Response struct {
	msg string
	err error
}

type DnodeConn struct {
	dnid     string
	conn     net.Conn
	sender   chan []byte
	receiver chan *Response
}

func compareVid(vid1, vid2 []byte) int {
	return bytes.Compare(vid1, vid2)
}

// if allowSame is false, refuse to insert into a vnode if the rtable exists
// a vnode with the same id.
// return true if vnode is inserted into rtable's vns.
func (rt *RTable) JoinVnode(vn *Vnode, allowSame bool) bool {
	// find insertion postion
	pos := sort.Search(len(rt.vns), func(i int) bool {
		return compareVid(rt.vns[i].id, vn.id) >= 0
	})

	if !allowSame {
		if pos < len(rt.vns) && compareVid(rt.vns[pos].id, vn.id) == 0 {
			return false
		}
	}

	// Insert new virtual node into route-table
	if pos == len(rt.vns) {
		rt.vns = append(rt.vns, vn)
	} else {
		rt.vns = append(rt.vns, nil)
		copy(rt.vns[pos+1:], rt.vns[pos:])
		rt.vns[pos] = vn
	}
	return true
}

// Find the vnode with id of keyhash
func (rt *RTable) Search(keyhash []byte) (int, *Vnode) {
	pos := sort.Search(len(rt.vns), func(i int) bool {
		return compareVid(rt.vns[i].id, keyhash) >= 0
	})
	if pos < len(rt.vns) && compareVid(rt.vns[pos].id, keyhash) == 0 {
		return pos, rt.vns[pos]
	} else {
		return -1, nil
	}
}

// Find the vnode who stores the key with hash of keyhash
func (rt *RTable) StoreSearch(keyhash []byte) *Vnode {
	if len(rt.vns) == 0 {
		return nil
	}
	pos := sort.Search(len(rt.vns), func(i int) bool {
		return compareVid(rt.vns[i].id, keyhash) >= 0
	})
	return rt.vns[pos%len(rt.vns)]
}

func buildDnodeConn(nid string) (net.Conn, error) {
	pn, pok := PnodeMap[nid]
	if !pok {
		return nil, fmt.Errorf("pnode with id=%s not found", nid)
	}
	addr, err := net.ResolveTCPAddr("tcp", pn.bindAddr)
	if err != nil {
		return nil, err
	}
	return net.DialTCP("tcp", nil, addr)
}

func DnodeMsgSender(dnid string, msg []byte) error {
	if dnconn, err := getDnodeConn(dnid); err != nil {
		return err
	} else {
		// TODO: error handling. e.g. broken connection, write failed etc.
		// FIXME benchmark shows great latency using channel way: dnconn.sender <- msg
		go func() {
			dnconn.conn.Write(msg)
		}()
	}
	return nil
}

func getDnodeConn(nid string) (*DnodeConn, error) {
	dnconn, ok := DnConns[nid]
	if !ok {
		conn, err := buildDnodeConn(nid)
		if err != nil {
			return nil, err
		}

		dnconn = &DnodeConn{
			dnid:     nid,
			conn:     conn,
			sender:   make(chan []byte),
			receiver: make(chan *Response),
		}
		DnConns[nid] = dnconn
		dnconn.LifeCycle()
	}
	return dnconn, nil
}

func (dnconn *DnodeConn) heartbeat() {
	rawmsg := &BasicMsg{
		cmdType: dmq.IDMsgCmdHeartbeatMsg,
		bodyLen: 0,
		extra:   dmq.IDMsgExtraNone,
		items:   make(map[uint8]string),
	}
	bmsg := binaryMsgEncode(rawmsg)
	dnconn.sender <- bmsg
}

func (dnconn *DnodeConn) HeartbeatRoutine(interval int) {
	ticker := time.NewTicker(time.Second * time.Duration(interval))
	for {
		<-ticker.C
		dnconn.heartbeat()
	}
}

func (dnconn *DnodeConn) LifeCycle() {
	go dnconn.HeartbeatRoutine(HbIntervalToDN)

	go func() {
		for {
			select {
			case msg := <-dnconn.sender:
				dnconn.conn.Write(msg)
			case resp := <-dnconn.receiver:
				if resp.err != nil {
					log.Debug("dnconn to %s receive err: %v", dnconn.dnid, resp.err)

					// ignore TCP connection close error
					dnconn.conn.Close()
					delete(DnConns, dnconn.dnid)
					return
				} else {
					log.Debug("receive msg: '%s' from dnconn %s", resp.msg, dnconn.dnid)
				}
			}
		}
	}()

	go func() {
		for {
			b := make([]byte, 2048)
			_, err := dnconn.conn.Read(b)
			dnconn.receiver <- &Response{msg: string(b), err: err}
			if err != nil {
				return
			}
		}
	}()
}
