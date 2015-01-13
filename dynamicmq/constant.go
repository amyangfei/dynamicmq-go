package dynamicmq

var (
	Crlf string = "\r\n"
)

var (
	EtcdConnectorType  = "connector"
	EtcdAuthServerType = "authsrv"
	EtcdDispatcherType = "dispatcher"
)

// Etcd name mapping
var (
	ConnSubAddr   = "sub_addr"
	ConnRouteAddr = "route_addr"
	ConnCapacity  = "capacity"
	ConnLoad      = "load"
	ConnStatus    = "status"

	DispConnId = "conn_id"
)

var (
	ConnectorNodeIdSize = 8
	SubClientIdSize     = 12
)

// Message protocol from Matcher to Dispatcher
var (
	MDMsgCmdSize        uint16 = 1
	MDMsgBodySize       uint16 = 2
	MDMsgExtraSize      uint16 = 1
	MDMsgHeaderSize     uint16 = MDMsgCmdSize + MDMsgBodySize + MDMsgExtraSize
	MDMsgItemIdSize     uint16 = 1
	MDMsgItemBodySize   uint16 = 2
	MDMsgItemHeaderSize uint16 = MDMsgItemIdSize + MDMsgItemBodySize
	MDMsgMaxBodyLen     uint16 = 2000

	// command
	MDMsgCmdPush uint8 = 1

	// Extra Field
	MDMsgExtraSendSingle  uint8 = 0x01
	MDMsgExtraSendMulHead uint8 = 0x02
	MDMsgExtraSendMulMid  uint8 = 0x04
	MDMsgExtraSendMulTail uint8 = 0x08

	// Message body item id
	MDMsgItemMsgidId   uint8 = 1
	MDMsgItemPayloadId uint8 = 2
	MDMsgItemSubListId uint8 = 3

	// Message body item size restriction
	MDMsgItemMsgidSize  uint16 = 12
	MDMsgItemMaxPayload uint16 = 256

	MDMsgSubInfoSep = ","
)
