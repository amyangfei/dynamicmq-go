package main

import (
	"encoding/binary"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
)

// BasicMsg struct
type BasicMsg struct {
	cmdType uint8
	bodyLen uint16
	extra   uint8
	items   map[uint8]string
}

func basicHeartbeatMsg() *BasicMsg {
	// FIXME: here if we use a global variable, go test will faile
	HeartbeatMsg := &BasicMsg{
		cmdType: dmq.DRMsgCmdHeartbeat,
		bodyLen: 0,
		extra:   dmq.DRMsgExtraNone,
		items:   map[uint8]string{},
	}
	return HeartbeatMsg
}

func basicHandshakeMsg() *BasicMsg {
	HandshakeMsg := &BasicMsg{
		cmdType: dmq.DRMsgCmdHandshake,
		bodyLen: 0,
		extra:   dmq.DRMsgExtraNone,
		items:   map[uint8]string{},
	}
	return HandshakeMsg
}

func binaryMsgEncode(msg *BasicMsg) []byte {
	bmsg := make([]byte, dmq.DRMsgHeaderSize)
	bmsg[0] = msg.cmdType
	binary.BigEndian.PutUint16(bmsg[1:], msg.bodyLen)
	bmsg[dmq.DRMsgCmdSize+dmq.DRMsgBodySize] = msg.extra
	var bodyLen uint16
	for itemid, item := range msg.items {
		bmsg = append(bmsg, itemid)
		bItemLen := make([]byte, dmq.DRMsgItemBodySize)
		binary.BigEndian.PutUint16(bItemLen, uint16(len(item)))
		bmsg = append(bmsg, bItemLen...)
		bmsg = append(bmsg, item...)
		bodyLen += dmq.DRMsgItemHeaderSize + uint16(len(item))
	}
	if msg.bodyLen != bodyLen {
		binary.BigEndian.PutUint16(bmsg[1:], bodyLen)
	}
	return bmsg
}
