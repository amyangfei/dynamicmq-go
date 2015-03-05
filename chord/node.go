package chord

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"sort"
)

func (n *Node) init(conf *NodeConfig) {
	n.config = conf
	n.vnodes = make([]*localVnode, conf.NumVnodes)

	for i := 0; i < conf.NumVnodes; i++ {
		lvn := &localVnode{
			node: n,
		}
		n.vnodes[i] = lvn
		lvn.init(i)
	}

	sort.Sort(n)
}

// Len is the number of vnodes
func (n *Node) Len() int {
	return len(n.vnodes)
}

// Less returns whether the vnode with index i should sort
// before the vnode with index j.
func (n *Node) Less(i, j int) bool {
	return bytes.Compare(n.vnodes[i].Id, n.vnodes[j].Id) == -1
}

// Swap swaps the vnodes with indexes i and j.
func (n *Node) Swap(i, j int) {
	n.vnodes[i], n.vnodes[j] = n.vnodes[j], n.vnodes[i]
}

func (n *Node) serfStart(c chan Notification) {
	args := make([]string, 0)
	args = append(args, "agent")
	if n.config.serf.NodeName != "" {
		args = append(args, fmt.Sprintf("-node=%s", n.config.serf.NodeName))
	}
	if n.config.serf.BindAddr != "" {
		args = append(args, fmt.Sprintf("-bind=%s", n.config.serf.BindAddr))
	}
	if n.config.serf.RPCAddr != "" {
		args = append(args, fmt.Sprintf("-rpc-addr=%s", n.config.serf.RPCAddr))
	}
	if n.config.serf.ConfigFile != "" {
		args = append(args, fmt.Sprintf("-config-file=%s", n.config.serf.ConfigFile))
	}
	if len(n.config.serf.Args) > 0 {
		args = append(args, n.config.serf.Args...)
	}

	cmd := exec.Command(n.config.serf.BinPath, args...)
	cmd.Env = os.Environ()[:]
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out

	go func() {
		if err := cmd.Run(); err != nil {
			c <- Notification{err: err, msg: out.String()}
		}
	}()
}

func (n *Node) serfStop() error {
	args := make([]string, 0)
	args = append(args, "leave")
	if n.config.serf.RPCAddr != "" {
		args = append(args, fmt.Sprintf("-rpc-addr=%s", n.config.serf.RPCAddr))
	}

	cmd := exec.Command(n.config.serf.BinPath, args...)
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
	if n.config.serf.RPCAddr != "" {
		args = append(args, fmt.Sprintf("-rpc-addr=%s", n.config.serf.RPCAddr))
	}
	args = append(args, addr)

	cmd := exec.Command(n.config.serf.BinPath, args...)
	cmd.Env = os.Environ()[:]
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%v: %s", err, out.String())
	}

	return nil
}

func (n *Node) Shutdown() error {
	err := n.serfStop()
	return err
}
