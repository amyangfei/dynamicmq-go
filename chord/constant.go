package chord

import (
	"errors"
)

// Serf event-handler helper
var (
	cfgSectNode    = "node"
	cfgItemRpcaddr = "rpc_addr"
)

var (
	serfAgentAlive = "alive"
)

var (
	serfUserevNodeinfo  = "nodeinfo"
	serfUserevVnodeinfo = "vnodeinfo"
)

var (
	errProcessLater = errors.New("process Later")
)
