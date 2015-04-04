package chord

import (
	"crypto/sha1"
	"hash"
	"io"
	"math/big"
)

type Notification struct {
	Err error
	Msg string
}

type SerfConfig struct {
	BinPath    string   // Path of serf binary
	NodeName   string   // Serf agent node name
	BindAddr   string   // Serf agent bind address and port
	RPCAddr    string   // Serf RPC communications address and port
	Args       []string // some other args
	EvHandler  string   //event handler
	ConfigFile string   // Path of configuration file that serf agent will load
}

type NodeConfig struct {
	Serf          *SerfConfig
	Hostname      string           // Local host name
	BindAddr      string           // Addr for message dispatching
	WorkDir       string           // working dir
	RPCAddr       string           // Addr for local serf agent communication
	NumVnodes     int              // Number of vnodes per physical node
	NumSuccessors int              // Number of successors to maintain
	HashFunc      func() hash.Hash // Hash function to use
	HashBits      int              // Bit size of hash function
	StartHash     []byte           // start hash value in hash ring
	maxhash       *big.Int         // max hash value in hash ring
	step          *big.Int         // hash interval for virtual node
	Entrypoint    string           // arbitrary BindAddr of other node in cluseter, empty for the first launched node
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

// Contains a virtual node
type localVnode struct {
	Vnode
	node       *Node
	successors []*Vnode
	// predecessor *Vnode
}

// Node represents a physical node. It has NumVnodes of vnodes.
type Node struct {
	config *NodeConfig
	Vnodes []*localVnode
}

func DefaultConfig(hostname, serfname string) *NodeConfig {
	return &NodeConfig{
		Serf: &SerfConfig{
			BinPath:    "/usr/local/bin/serf",
			NodeName:   serfname,
			BindAddr:   "0.0.0.0:7946",
			RPCAddr:    "127.0.0.1:7373",
			EvHandler:  "./serfev_handler.py",
			ConfigFile: "",
		},
		Hostname:      hostname,
		BindAddr:      "0.0.0.0:5000",
		RPCAddr:       "127.0.0.1:5500",
		NumVnodes:     16,
		NumSuccessors: 3,
		HashFunc:      sha1.New,
		HashBits:      160,
		StartHash:     make([]byte, 20),
	}
}

func initHashConstant(conf *NodeConfig) {
	maxHashBytes := make([]byte, conf.HashBits/8+1)
	maxHashBytes[0] = 1
	maxhash := big.NewInt(0)
	maxhash.SetBytes(maxHashBytes)

	minusBi := big.NewInt(int64(conf.NumVnodes))
	step := big.NewInt(0)
	step.SetBytes(maxhash.Bytes())
	step.Div(step, minusBi)

	conf.maxhash = maxhash
	conf.step = step
}

// Create a new Chord node
func Create(conf *NodeConfig, c chan Notification, logger io.Writer) (*Node, error) {
	initHashConstant(conf)
	node := CreateNode(conf)
	// TODO: start routine running with serf agent
	node.serfStart(c, logger)
	return node, nil
}

// Joins an existing Chord ring
// serfAddr is an arbitrary serf bind address of nodes in Chord ring
func Join(node *Node, serfAddr string) error {
	return nil
}
