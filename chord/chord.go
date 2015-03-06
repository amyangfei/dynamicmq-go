package chord

import (
	"crypto/sha1"
	"hash"
	"io"
)

type Notification struct {
	err error
	msg string
}

type SerfConfig struct {
	BinPath    string   // Path of serf binary
	NodeName   string   // Serf agent node name
	BindAddr   string   // Serf agent bind address and port
	RPCAddr    string   // Serf  RPC communications address and port
	Args       []string // some other args
	EvHandler  string   //event handler
	ConfigFile string   // Path of configuration file that serf agent will load
}

type NodeConfig struct {
	serf          *SerfConfig
	Hostname      string           // Local host name
	BindAddr      string           // Addr for message dispatching
	WorkDir       string           // working dir
	RPCAddr       string           // Addr for local serf agent communication
	NumVnodes     int              // Number of vnodes per physical node
	NumSuccessors int              // Number of successors to maintain
	HashFunc      func() hash.Hash // Hash function to use
	hashBits      int              // Bit size of hash function
}

// Represents an Vnode
type Vnode struct {
	Id   []byte // Virtual ID
	Host string // Host identifier
}

// Router Table: stores all vnodes in chord hash ring
type RTable struct {
	vnodes []*Vnode
}

type localVnode struct {
	Vnode
	node        *Node
	successors  []*Vnode
	predecessor *Vnode
}

type Node struct {
	config *NodeConfig
	vnodes []*localVnode
}

func DefaultConfig(hostname, serfname string) *NodeConfig {
	return &NodeConfig{
		serf: &SerfConfig{
			NodeName:   serfname,
			BindAddr:   "0.0.0.0:7946",
			RPCAddr:    "127.0.0.1:7373",
			EvHandler:  "./serfev_handler.py",
			ConfigFile: "",
		},
		Hostname:      hostname,
		BindAddr:      "0.0.0.0:5000",
		RPCAddr:       "127.0.0.1:5500",
		NumVnodes:     8,
		NumSuccessors: 3,
		HashFunc:      sha1.New,
		hashBits:      160,
	}
}

// Create a new Chord node
func Create(conf *NodeConfig, c chan Notification, logger io.Writer) (*Node, error) {
	node := &Node{}
	node.init(conf)
	// TODO: start routine running with serf agent
	node.serfStart(c, logger)
	return node, nil
}

// Joins an existing Chord ring
// serfAddr is an arbitrary serf bind address of nodes in Chord ring
func Join(node *Node, serfAddr string) error {
	return nil
}