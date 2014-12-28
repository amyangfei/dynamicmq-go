package main

// Message received by inner dispathcer header size
var (
	PubMsgCmdSize        uint16 = 1
	PubMsgBodySize       uint16 = 2
	PubMsgExtraSize      uint16 = 1
	PubMsgHeaderSize     uint16 = PubMsgCmdSize + PubMsgBodySize + PubMsgExtraSize
	PubMsgItemIdSize     uint16 = 1
	PubMsgItemBodySize   uint16 = 2
	PubMsgItemHeaderSize uint16 = PubMsgItemIdSize + PubMsgItemBodySize
	PubMsgMaxBodyLen     uint16 = 2000
)

var (
	PubMsgCmdPush uint8 = 1
)

var (
	PubMsgExtraSendSingle  uint8 = 0x01
	PubMsgExtraSendMulHead uint8 = 0x02
	PubMsgExtraSendMulMid  uint8 = 0x04
	PubMsgExtraSendMulTail uint8 = 0x08
)

var (
	PubMsgItemMsgidId   uint8 = 1
	PubMsgItemPayloadId uint8 = 2
	PubMsgItemSubListId uint8 = 3
)

var (
	PubMsgItemMsgidSize  uint16 = 24
	PubMsgItemMaxPayload uint16 = 256
)
