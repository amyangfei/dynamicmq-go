package chord

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
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

func (n *Node) serfStart(c chan Notification, logger io.Writer) {
	args := make([]string, 0)
	args = append(args, "agent")
	if n.config.Serf.NodeName != "" {
		args = append(args, fmt.Sprintf("-node=%s", n.config.Serf.NodeName))
	}
	if n.config.Serf.BindAddr != "" {
		args = append(args, fmt.Sprintf("-bind=%s", n.config.Serf.BindAddr))
	}
	if n.config.Serf.RPCAddr != "" {
		args = append(args, fmt.Sprintf("-rpc-addr=%s", n.config.Serf.RPCAddr))
	}
	if n.config.Serf.EvHandler != "" {
		args = append(args, fmt.Sprintf("-event-handler=%s", n.config.Serf.EvHandler))
	}
	if n.config.Serf.ConfigFile != "" {
		args = append(args, fmt.Sprintf("-config-file=%s", n.config.Serf.ConfigFile))
	}
	if len(n.config.Serf.Args) > 0 {
		args = append(args, n.config.Serf.Args...)
	}

	cmd := exec.Command(n.config.Serf.BinPath, args...)
	cmd.Env = os.Environ()[:]
	var out bytes.Buffer
	cmd.Stdout = logger
	cmd.Stderr = &out

	go func() {
		if err := cmd.Run(); err != nil {
			c <- Notification{Err: err, Msg: out.String()}
		}
	}()
}

func (n *Node) serfStop() error {
	args := make([]string, 0)
	args = append(args, "leave")
	if n.config.Serf.RPCAddr != "" {
		args = append(args, fmt.Sprintf("-rpc-addr=%s", n.config.Serf.RPCAddr))
	}

	cmd := exec.Command(n.config.Serf.BinPath, args...)
	cmd.Env = os.Environ()[:]
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out

	if err := cmd.Run(); err != nil {
		// "client closed" error may occur, ignore it
	}

	fmt.Printf("stop serf output: %s\n", out.String())

	return nil
}

// Call This function to tell serf running on this node to join the cluster
// addr is an arbitrary serf bind address of nodes in Chord ring
func (n *Node) serfJoin(addr string) error {
	args := make([]string, 0)
	args = append(args, "join")
	if n.config.Serf.RPCAddr != "" {
		args = append(args, fmt.Sprintf("-rpc-addr=%s", n.config.Serf.RPCAddr))
	}
	args = append(args, addr)

	cmd := exec.Command(n.config.Serf.BinPath, args...)
	cmd.Env = os.Environ()[:]
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%v: %s", err, out.String())
	}

	return nil
}

// Call This function to send serf user event to serf cluster
// evname, represents for event type, including:
// 	 nodeinfo: information of chord physical node
// 	 vnodeinfo: information of all vnodes that belongs to one chord physical node
// coalesce:
//   If coalesce is true, if many events of the same name are received within a
//   short amount of time, the event handler is only invoked once.
func (n *Node) serfUserEvent(evname, payload string, coalesce bool, c chan Notification) {
	args := make([]string, 0)
	args = append(args, "event")
	if !coalesce {
		args = append(args, "-coalesce=false")
	}
	if n.config.Serf.RPCAddr != "" {
		args = append(args, fmt.Sprintf("-rpc-addr=%s", n.config.Serf.RPCAddr))
	}
	args = append(args, evname)
	args = append(args, payload)

	cmd := exec.Command(n.config.Serf.BinPath, args...)
	cmd.Env = os.Environ()[:]
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out

	if err := cmd.Run(); err != nil {
		go func() {
			c <- Notification{Err: err, Msg: out.String()}
		}()
	}
}

func (n *Node) Shutdown() error {
	err := n.serfStop()
	return err
}

// Let serf agent join the serf cluster,
// then broadcast local information to cluster
func (n *Node) schdule() error {
	var serfBind string = "127.0.0.1:7497"
	if err := n.serfJoin(serfBind); err != nil {
		return err
	}
	return nil
}
