package chord

import (
	"crypto/sha1"
	"github.com/op/go-logging"
	"hash"
	"math/big"
	"runtime"
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
	HostIp        string           // Host ip
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

	TCPRecvBufSize int
	TCPSendBufSize int
	TCPBufInsNum   int
	TCPBufioNum    int
}

// Represents an Vnode
type Vnode struct {
	Id    []byte    // Virtual ID
	Pnode *PeerNode // physical node pointer
}

// Router Table: stores all vnodes in chord hash ring
type RTable struct {
	vnodes []*Vnode
	peers  []*PeerNode
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
	config  *NodeConfig
	LVnodes []*localVnode
	log     *logging.Logger
	rtable  *RTable
}

// represents for peer physical node
type PeerNode struct {
	Hostname  string
	SerfNode  string
	BindAddr  string
	RPCAddr   string
	StartHash []byte
}

func DefaultConfig(hostname, serfname string) *NodeConfig {
	conf := &NodeConfig{
		Serf: &SerfConfig{
			BinPath:    "/usr/local/bin/serf",
			NodeName:   serfname,
			BindAddr:   "0.0.0.0:7946",
			RPCAddr:    "127.0.0.1:7373",
			EvHandler:  "./serfev_handler.py",
			ConfigFile: "",
		},
		Hostname:       hostname,
		HostIp:         "127.0.0.1",
		BindAddr:       "0.0.0.0:5000",
		RPCAddr:        "0.0.0.0:5500",
		NumVnodes:      16,
		NumSuccessors:  3,
		HashFunc:       sha1.New,
		HashBits:       160,
		StartHash:      make([]byte, 20),
		TCPRecvBufSize: 2048,
		TCPSendBufSize: 2048,
		TCPBufInsNum:   runtime.NumCPU(),
		TCPBufioNum:    64,
	}
	initHashConstant(conf)

	return conf
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
func Create(conf *NodeConfig) (*Node, error) {
	initHashConstant(conf)
	node := CreateNode(conf)
	return node, nil
}
