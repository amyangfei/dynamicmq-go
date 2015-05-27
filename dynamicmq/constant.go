package dynamicmq

var (
	Crlf = "\r\n"
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

	DispConnID   = "conn_id"
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

// Action name mapping of redis notification
const (
	RedisNotifySet = "set"
	RedisNotifyDel = "del"
)

// attribute notification channel pattern in redis
const (
	RedisSubAttrPattern = "__keyspace*__:/sub/attr/*"
)

var (
	ConnectorNodeIDSize  = 8
	DispatcherNodeIDSize = 8
	SubClientIDSize      = 12
)

// Message protocol from Matcher to Dispatcher
var (
	MDMsgCmdSize        uint16 = 1
	MDMsgBodySize       uint16 = 2
	MDMsgExtraSize      uint16 = 1
	MDMsgHeaderSize            = MDMsgCmdSize + MDMsgBodySize + MDMsgExtraSize
	MDMsgItemIDSize     uint16 = 1
	MDMsgItemBodySize   uint16 = 2
	MDMsgItemHeaderSize        = MDMsgItemIDSize + MDMsgItemBodySize
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
	MDMsgItemMsgidID   uint8 = 1
	MDMsgItemPayloadID uint8 = 2
	MDMsgItemSubListID uint8 = 3

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
	DRMsgHeaderSize            = DRMsgCmdSize + DRMsgBodySize + DRMsgExtraSize
	DRMsgItemIDSize     uint16 = 1
	DRMsgItemBodySize   uint16 = 2
	DRMsgItemHeaderSize        = DRMsgItemIDSize + DRMsgItemBodySize
	DRMsgMaxBodyLen     uint16 = 2000

	// command
	DRMsgCmdPushMsg         = MDMsgCmdPushMsg   // 1
	DRMsgCmdHeartbeat       = MDMsgCmdHeartbeat // 2
	DRMsgCmdHandshake uint8 = 3

	// Extra Field
	DRMsgExtraNone        uint8 = 0x00
	DRMsgExtraSendSingle  uint8 = 0x01
	DRMsgExtraSendMulHead uint8 = 0x02
	DRMsgExtraSendMulMid  uint8 = 0x04
	DRMsgExtraSendMulTail uint8 = 0x08
	DRMsgExtraRedirect    uint8 = 0x10

	// Message body item id
	DRMsgItemMsgidID     uint8 = 1
	DRMsgItemPayloadID   uint8 = 2
	DRMsgItemSubListID   uint8 = 3
	DRMsgItemTimestampID uint8 = 4
	DRMsgItemDispidID    uint8 = 5

	// Message body item size restriction
	DRMsgItemMsgidSize  uint16 = 12
	DRMsgItemMaxPayload uint16 = 256
	DRMsgItemTsSize     uint16 = 8
	DRMsgItemDispIDSize        = uint16(DispatcherNodeIDSize)
)

// Message protocol from local serf handler or datanode to datanode
var (
	SDDMsgCmdSize        uint16 = 1
	SDDMsgBodySize       uint16 = 2
	SDDMsgExtraSize      uint16 = 1
	SDDMsgHeaderSize            = SDDMsgCmdSize + SDDMsgBodySize + SDDMsgExtraSize
	SDDMsgItemIDSize     uint16 = 1
	SDDMsgItemBodySize   uint16 = 2
	SDDMsgItemHeaderSize        = SDDMsgItemIDSize + SDDMsgItemBodySize
	SDDMsgMaxBodyLen     uint16 = 2000

	// command
	SDDMsgCmdNodeInfo  uint8 = 1
	SDDMsgCmdVNodeInfo uint8 = 2

	// Message body item id
	SDDMsgItemHostnameID  uint8 = 1
	SDDMsgItemBindAddrID  uint8 = 2
	SDDMsgItemRPCAddrID   uint8 = 3
	SDDMsgItemStartHashID uint8 = 4
	SDDMsgItemSerfNodeID  uint8 = 5
	SDDMsgItemVNodeListID uint8 = 6
)

// Message protocol from publisher to matching service
var (
	PMMsgCmdSize        uint16 = 1
	PMMsgBodySize       uint16 = 2
	PMMsgExtraSize      uint16 = 1
	PMMsgHeaderSize            = PMMsgCmdSize + PMMsgBodySize + PMMsgExtraSize
	PMMsgItemIDSize     uint16 = 1
	PMMsgItemBodySize   uint16 = 2
	PMMsgItemHeaderSize        = PMMsgItemIDSize + PMMsgItemBodySize
	PMMsgMaxBodyLen     uint16 = 2000

	// command
	PMMsgCmdPushMsg uint8 = 1

	// Extra Field
	PMMsgExtraNone uint8 = 0x00

	// Message body item id
	PMMsgItemAttributeID uint8 = 1
	PMMsgItemPayloadID   uint8 = 2
)

// Message protocol from indexnode to datanode in matching service
var (
	IDMsgCmdSize        uint16 = 1
	IDMsgBodySize       uint16 = 2
	IDMsgExtraSize      uint16 = 1
	IDMsgHeaderSize            = IDMsgCmdSize + IDMsgBodySize + IDMsgExtraSize
	IDMsgItemIDSize     uint16 = 1
	IDMsgItemBodySize   uint16 = 2
	IDMsgItemHeaderSize        = IDMsgItemIDSize + IDMsgItemBodySize
	IDMsgMaxBodyLen     uint16 = 2000

	// command
	IDMsgCmdPushMsg      uint8 = 1
	IDMsgCmdHeartbeatMsg uint8 = 2

	// Extra Field
	IDMsgExtraNone uint8 = 0x00

	// Message body item id
	IDMsgItemAttributeID uint8 = 1
	IDMsgItemPayloadID   uint8 = 2
	IDMsgMessageIDID     uint8 = 3
	IDMsgClientListIDID  uint8 = 4
)

// attribute type in subscription
var (
	AttrUseField = map[string]int{
		"strval": 1,
		"range":  2,
		"extra":  3,
	}
	AttrUseStr   = "strval"
	AttrUseRange = "range"
	AttrUseExtra = "extra"
)
