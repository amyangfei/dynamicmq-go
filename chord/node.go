package chord

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
)

func chgWorkdir(path string) error {
	if path == "" {
		return nil
	}
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			os.MkdirAll(path, 0755)
		}
	}
	return os.Chdir(path)
}

func createSerfevHelper(conf *NodeConfig) error {
	fname := fmt.Sprintf("%s.evhelper.ini", conf.Serf.NodeName)
	f, err := os.OpenFile(fname, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	w.WriteString(fmt.Sprintf("[%s]\n", cfg_sect_node))
	w.WriteString(fmt.Sprintf("%s = %s\n", cfg_item_rpcaddr, conf.RPCAddr))
	w.Flush()
	return nil
}

func CreateNode(conf *NodeConfig) *Node {
	node := &Node{config: conf}
	node.init()
	return node
}

func (n *Node) init() {
	n.Vnodes = make([]*localVnode, n.config.NumVnodes)

	// change working dir
	chgWorkdir(n.config.WorkDir)

	createSerfevHelper(n.config)

	curHash := n.config.StartHash[:]
	for i := 0; i < n.config.NumVnodes; i++ {
		lvn := &localVnode{
			node: n,
		}
		n.Vnodes[i] = lvn
		lvn.init(curHash)
		curHash = HashJump(curHash, n.config.step, n.config.maxhash)
	}
}

// Len is the number of vnodes
func (n *Node) Len() int {
	return len(n.Vnodes)
}

// Less returns whether the vnode with index i should sort
// before the vnode with index j.
func (n *Node) Less(i, j int) bool {
	return bytes.Compare(n.Vnodes[i].Id, n.Vnodes[j].Id) == -1
}

// Swap swaps the vnodes with indexes i and j.
func (n *Node) Swap(i, j int) {
	n.Vnodes[i], n.Vnodes[j] = n.Vnodes[j], n.Vnodes[i]
}

func (n *Node) SerfStart(c chan Notification, logger io.Writer) {
	params := make(map[string]string)
	if n.config.Serf.NodeName != "" {
		params["node"] = n.config.Serf.NodeName
	}
	if n.config.Serf.BindAddr != "" {
		params["bind"] = n.config.Serf.BindAddr
	}
	if n.config.Serf.RPCAddr != "" {
		params["rpc-addr"] = n.config.Serf.RPCAddr
	}
	if n.config.Serf.EvHandler != "" {
		params["event-handler"] = n.config.Serf.EvHandler
	}
	if n.config.Serf.ConfigFile != "" {
		params["config-file"] = n.config.Serf.ConfigFile
	}

	serfStart(c, logger, n.config.Serf.BinPath, params, n.config.Serf.Args)
}

func (n *Node) SerfStop() error {
	return serfStop(n.config.Serf.BinPath, n.config.Serf.RPCAddr)
}

// Call This function to tell serf running on this node to join the cluster
// addr is an arbitrary serf bind address of nodes in Chord ring
func (n *Node) SerfJoin(addr string) error {
	return serfJoin(n.config.Serf.BinPath, n.config.Serf.RPCAddr, addr)
}

// Call This function to send serf user event to serf cluster
// evname, represents for event type, including:
// 	 nodeinfo: information of chord physical node
// 	 vnodeinfo: information of all vnodes that belongs to one chord physical node
// coalesce:
//   If coalesce is true, if many events of the same name are received within a
//   short amount of time, the event handler is only invoked once.
func (n *Node) SerfUserEvent(evname, payload string, coalesce bool, c chan Notification) {
	params := make(map[string]string)
	params["coalesce"] = "false"
	if n.config.Serf.RPCAddr != "" {
		params["rpc-addr"] = n.config.Serf.RPCAddr
	}
	serfUserEvent(n.config.Serf.BinPath, evname, payload, params, c)
}

func (n *Node) Shutdown() error {
	err := n.SerfStop()
	return err
}

func (n *Node) serfSchdule(c chan Notification, logger io.Writer) error {
	// start serf agent
	n.SerfStart(c, logger)

	// try to detect member's aliveness for three times
	retry_count := 3
	for i := 0; i < retry_count; i++ {
		msg, err := checkMemberAlive(n.config.Serf.BinPath, n.config.Serf.RPCAddr)
		if err == nil {
			if strings.Contains(msg, n.config.Serf.NodeName) &&
				strings.Contains(msg, serf_agent_alive) {
				break
			}
		}
		if i == retry_count-1 {
			if err != nil {
				return fmt.Errorf("%v: %s", err, msg)
			} else {
				return fmt.Errorf("alive not detected: %s", msg)
			}
		}
	}

	// if not the first launched serf agent, join the cluster
	if n.config.Entrypoint != "" {
		if err := n.SerfJoin(n.config.Entrypoint); err != nil {
			return err
		}
	}

	// TODO: broadcast self node information

	return nil
}
