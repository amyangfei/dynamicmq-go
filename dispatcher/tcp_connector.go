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
	go RmHeartbeat2Conn(RouterMgr)
	return nil
}

func RmHeartbeat2Conn(rmgr *RouterManager) {
	ticker := time.NewTicker(time.Second * time.Duration(Config.HeartbeatIval))
	b := make([]byte, dmq.DRMsgItemTsSize)
	for {
		<-ticker.C
		now := time.Now().Unix()
		binary.BigEndian.PutUint64(b, uint64(now))
		HeartbeatMsg.items[dmq.DRMsgItemTimestampId] = string(b)
		HeartbeatMsg.items[dmq.DRMsgItemPayloadId] = "dynamic message queue"
		bmsg := binaryMsgEncode(HeartbeatMsg)
		rmgr.conn.Write(bmsg)
		log.Debug("send heartbeat to connector %s %s", rmgr.cid, bmsg)
	}
}
