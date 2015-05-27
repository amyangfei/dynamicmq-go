package main

import (
	"encoding/binary"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"net"
	"time"
)

type RouterManager struct {
	cid    string // Connector NodeId
	conn   net.Conn
	status int
}

func (rmgr *RouterManager) SendData(msg []byte) error {
	_, err := rmgr.conn.Write(msg)
	return err
}

func ConnToConnRouter(addr, cid string, rmgr *RouterManager, cfg *SrvConfig) error {
	log.Info("start tcp connection to router: %s", addr)
	raddr, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		return err
	}
	conn, err := net.DialTCP("tcp", nil, raddr)
	if err != nil {
		return err
	}

	rmgr.cid = cid
	rmgr.conn = conn
	rmgr.status = RmStatusOk

	go RmHandShake2Conn(rmgr, cfg)
	go RmHeartbeat2Conn(rmgr, cfg)

	return nil
}

func RmHandShake2Conn(rmgr *RouterManager, cfg *SrvConfig) {
	hsMsg := BasicHandshakeMsg()
	hsMsg.items[dmq.DRMsgItemDispidID] = cfg.NodeId
	bmsg := binaryMsgEncode(hsMsg)
	rmgr.SendData(bmsg)
}

func RmHeartbeat2Conn(rmgr *RouterManager, cfg *SrvConfig) {
	ticker := time.NewTicker(time.Second * time.Duration(cfg.HeartbeatIval))
	b := make([]byte, dmq.DRMsgItemTsSize)
	for {
		<-ticker.C
		now := time.Now().Unix()
		binary.BigEndian.PutUint64(b, uint64(now))
		hbMsg := BasicHeartbeatMsg()
		hbMsg.items[dmq.DRMsgItemTimestampID] = string(b)
		bmsg := binaryMsgEncode(hbMsg)
		rmgr.SendData(bmsg)
		log.Debug("send heartbeat to connector %s", rmgr.cid)
	}
}

func RmSendMsg2Conn(rmgr *RouterManager, msg []byte) {
	go func() {
		// From golang document: http://golang.org/pkg/net/
		// Multiple goroutines may invoke methods on a Conn simultaneously.
		wlen, err := rmgr.conn.Write(msg)
		if err != nil {
			log.Error("rm send msg with error: %v", err)
		}
		if wlen != len(msg) {
			log.Error("rm send msg with length %d should be %d", wlen, len(msg))
		}
	}()
}
