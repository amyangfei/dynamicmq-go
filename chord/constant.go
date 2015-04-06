package chord

import (
	"errors"
)

// Serf event-handler helper
var (
	cfg_sect_node    = "node"
	cfg_item_rpcaddr = "rpc_addr"
)

var (
	serf_agent_alive = "alive"
)

var (
	serf_userev_nodeinfo  = "nodeinfo"
	serf_userev_vnodeinfo = "vnodeinfo"
)

var (
	ProcessLater = errors.New("process Later")
)
