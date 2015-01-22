package dynamicmq

var (
	Crlf string = "\r\n"
)

var (
	EtcdConnectorType  = "conn"
	EtcdAuthServerType = "auth"
	EtcdDispatcherType = "disp"
	EtcdSubscriberType = "sub"
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
	ConnectorNodeIdSize  = 8
	DispatcherNodeIdSize = 8
	SubClientIdSize      = 12
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
	MDMsgCmdPushMsg uint8 = 1

	// Extra Field
	MDMsgExtraNone        uint8 = 0x00
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

// Message protocol from Dispatcher to Connector-Router or
// from Dispatcher to Dispatcher
var (
	DRMsgCmdSize        uint16 = 1
	DRMsgBodySize       uint16 = 2
	DRMsgExtraSize      uint16 = 1
	DRMsgHeaderSize     uint16 = DRMsgCmdSize + DRMsgBodySize + DRMsgExtraSize
	DRMsgItemIdSize     uint16 = 1
	DRMsgItemBodySize   uint16 = 2
	DRMsgItemHeaderSize uint16 = DRMsgItemIdSize + DRMsgItemBodySize
	DRMsgMaxBodyLen     uint16 = 2000

	// command
	DRMsgCmdHandshake uint8 = 1
	DRMsgCmdHeartbeat uint8 = 2
	DRMsgCmdPushMsg   uint8 = 3

	// Extra Field
	DRMsgExtraNone        uint8 = 0x00
	DRMsgExtraSendSingle  uint8 = 0x01
	DRMsgExtraSendMulHead uint8 = 0x02
	DRMsgExtraSendMulMid  uint8 = 0x04
	DRMsgExtraSendMulTail uint8 = 0x08

	// Message body item id
	DRMsgItemMsgidId     uint8 = 1
	DRMsgItemPayloadId   uint8 = 2
	DRMsgItemSubListId   uint8 = 3
	DRMsgItemTimestampId uint8 = 4
	DRMsgItemDispidId    uint8 = 5

	// Message body item size restriction
	DRMsgItemMsgidSize  uint16 = 12
	DRMsgItemMaxPayload uint16 = 256
	DRMsgItemTsSize     uint16 = 8
	DRMsgItemDispIdSize uint16 = uint16(DispatcherNodeIdSize)

	DRMsgSubInfoSep = ","
)
