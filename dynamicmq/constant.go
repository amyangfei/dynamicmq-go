package dynamicmq

var (
	Crlf string = "\r\n"
)

var (
	EtcdConnectorType  = "conn"
	EtcdAuthServerType = "auth"
	EtcdDispatcherType = "disp"
	EtcdSubscriberType = "sub"
	EtcdIndexInfoType  = "idx"
	EtcdDataNodeType   = "datn"
)

// Etcd name mapping
var (
	ConnSubAddr   = "sub_addr"
	ConnRouteAddr = "route_addr"
	ConnCapacity  = "capacity"
	ConnLoad      = "load"
	ConnStatus    = "status"

	DispConnId   = "conn_id"
	DispBindAddr = "bind_addr"

	IdxAttrLower = "lower"
	IdxAttrUpper = "upper"

	DataPnodeStatus  = "status"
	DataPnodePubAddr = "pub_addr"

	DataPnode = "pnode"
	DataVnode = "vnode"

	DataNodeStatusNew     = "new"
	DataNodeStatusActive  = "active"
	DataNodeStatusOffline = "offline"
)

// Action name mapping in etcd
const (
	EtcdActionGet              = "get"
	EtcdActionCreate           = "create"
	EtcdActionSet              = "set"
	EtcdActionUpdate           = "update"
	EtcdActionDelete           = "delete"
	EtcdActionCompareAndSwap   = "compareAndSwap"
	EtcdActionCompareAndDelete = "compareAndDelete"
	EtcdActionExpire           = "expire"
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
	MDMsgCmdPushMsg   uint8 = 1
	MDMsgCmdHeartbeat uint8 = 2

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
	DRMsgCmdPushMsg   uint8 = MDMsgCmdPushMsg   // 1
	DRMsgCmdHeartbeat uint8 = MDMsgCmdHeartbeat // 2
	DRMsgCmdHandshake uint8 = 3

	// Extra Field
	DRMsgExtraNone        uint8 = 0x00
	DRMsgExtraSendSingle  uint8 = 0x01
	DRMsgExtraSendMulHead uint8 = 0x02
	DRMsgExtraSendMulMid  uint8 = 0x04
	DRMsgExtraSendMulTail uint8 = 0x08
	DRMsgExtraRedirect    uint8 = 0x10

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
)

// Message protocol from local serf handler or datanode to datanode
var (
	SDDMsgCmdSize        uint16 = 1
	SDDMsgBodySize       uint16 = 2
	SDDMsgExtraSize      uint16 = 1
	SDDMsgHeaderSize     uint16 = SDDMsgCmdSize + SDDMsgBodySize + SDDMsgExtraSize
	SDDMsgItemIdSize     uint16 = 1
	SDDMsgItemBodySize   uint16 = 2
	SDDMsgItemHeaderSize uint16 = SDDMsgItemIdSize + SDDMsgItemBodySize
	SDDMsgMaxBodyLen     uint16 = 2000

	// command
	SDDMsgCmdNodeInfo  uint8 = 1
	SDDMsgCmdVNodeInfo uint8 = 2

	// Message body item id
	SDDMsgItemHostnameId  uint8 = 1
	SDDMsgItemBindAddrId  uint8 = 2
	SDDMsgItemRPCAddrId   uint8 = 3
	SDDMsgItemStartHashId uint8 = 4
	SDDMsgItemSerfNodeId  uint8 = 5
	SDDMsgItemVNodeListId uint8 = 6
)

// Message protocol from publisher to matching service
var (
	PMMsgCmdSize        uint16 = 1
	PMMsgBodySize       uint16 = 2
	PMMsgExtraSize      uint16 = 1
	PMMsgHeaderSize     uint16 = PMMsgCmdSize + PMMsgBodySize + PMMsgExtraSize
	PMMsgItemIdSize     uint16 = 1
	PMMsgItemBodySize   uint16 = 2
	PMMsgItemHeaderSize uint16 = PMMsgItemIdSize + PMMsgItemBodySize
	PMMsgMaxBodyLen     uint16 = 2000

	// command
	PMMsgCmdPushMsg uint8 = 1

	// Extra Field
	PMMsgExtraNone uint8 = 0x00

	// Message body item id
	PMMsgItemAttributeId uint8 = 1
	PMMsgItemPayloadId   uint8 = 2
)

// Message protocol from indexnode to datanode in matching service
var (
	IDMsgCmdSize        uint16 = 1
	IDMsgBodySize       uint16 = 2
	IDMsgExtraSize      uint16 = 1
	IDMsgHeaderSize     uint16 = IDMsgCmdSize + IDMsgBodySize + IDMsgExtraSize
	IDMsgItemIdSize     uint16 = 1
	IDMsgItemBodySize   uint16 = 2
	IDMsgItemHeaderSize uint16 = IDMsgItemIdSize + IDMsgItemBodySize
	IDMsgMaxBodyLen     uint16 = 2000

	// command
	IDMsgCmdPushMsg      uint8 = 1
	IDMsgCmdHeartbeatMsg uint8 = 2

	// Extra Field
	IDMsgExtraNone uint8 = 0x00

	// Message body item id
	IDMsgItemAttributeId uint8 = 1
	IDMsgItemPayloadId   uint8 = 2
	IDMsgMessageIdId     uint8 = 3
	IDMsgClientListIdId  uint8 = 4
)

// attribute type in subscription
var (
	AttrUseField = map[string]int{
		"strval": 1,
		"range":  2,
		"extra":  3,
	}
	AttrUseStr   string = "strval"
	AttrUseRange string = "range"
	AttrUseExtra string = "extra"
)
