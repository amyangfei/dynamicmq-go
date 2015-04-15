package main

import (
	"encoding/binary"
	"fmt"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
)

func AttrNameCombine(xattr, yattr string) string {
	if xattr < yattr {
		return fmt.Sprintf("%s-%s", xattr, yattr)
	} else {
		return fmt.Sprintf("%s-%s", yattr, xattr)
	}
}

func binaryMsgEncode(msg *BasicMsg) []byte {
	bmsg := make([]byte, dmq.MDMsgHeaderSize)
	bmsg[0] = msg.cmdType
	binary.BigEndian.PutUint16(bmsg[1:], msg.bodyLen)
	bmsg[dmq.MDMsgCmdSize+dmq.MDMsgBodySize] = msg.extra
	var bodyLen uint16 = 0
	for itemid, item := range msg.items {
		bmsg = append(bmsg, itemid)
		bItemLen := make([]byte, dmq.MDMsgItemBodySize)
		binary.BigEndian.PutUint16(bItemLen, uint16(len(item)))
		bmsg = append(bmsg, bItemLen...)
		bmsg = append(bmsg, item...)
		bodyLen += dmq.MDMsgItemHeaderSize + uint16(len(item))
	}
	if msg.bodyLen != bodyLen {
		binary.BigEndian.PutUint16(bmsg[1:], bodyLen)
	}
	return bmsg
}
