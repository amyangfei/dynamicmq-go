package main

import (
	"encoding/binary"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"net"
	"time"
)

type RouterManager struct {
	cid  string // Connector NodeId
	conn net.Conn
}

func (rmgr *RouterManager) SendData(msg []byte) error {
	_, err := rmgr.conn.Write(msg)
	return err
}

func ConnToConnRouter(addr, cid string) error {
	log.Info("start tcp connection to router: %s", addr)
	raddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return err
	}
	conn, err := net.DialTCP("tcp", nil, raddr)
	if err != nil {
		return err
	}
	RouterMgr = &RouterManager{
		conn: conn,
		cid:  cid,
	}
	go RmHandShake2Conn(RouterMgr)
	go RmHeartbeat2Conn(RouterMgr)
	return nil
}

func RmHandShake2Conn(rmgr *RouterManager) {
	HandshakeMsg.items[dmq.DRMsgItemDispidId] = Config.NodeId
	bmsg := binaryMsgEncode(HandshakeMsg)
	rmgr.SendData(bmsg)
}

func RmHeartbeat2Conn(rmgr *RouterManager) {
	ticker := time.NewTicker(time.Second * time.Duration(Config.HeartbeatIval))
	b := make([]byte, dmq.DRMsgItemTsSize)
	for {
		<-ticker.C
		now := time.Now().Unix()
		binary.BigEndian.PutUint64(b, uint64(now))
		HeartbeatMsg.items[dmq.DRMsgItemTimestampId] = string(b)
		bmsg := binaryMsgEncode(HeartbeatMsg)
		rmgr.SendData(bmsg)
		log.Debug("send heartbeat to connector %s", rmgr.cid)
	}
}
