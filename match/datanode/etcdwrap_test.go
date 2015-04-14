package main

import (
	"encoding/hex"
	"fmt"
	"github.com/amyangfei/dynamicmq-go/chord"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"testing"
)

var ecpool *dmq.EtcdClientPool

func init() {
	cfg := fakeSrvConfig()
	ecpool = dmq.NewEtcdClientPool(cfg.EtcdMachines, 1, 16)
}

func fakeSrvConfig() *SrvConfig {
	cfg := &SrvConfig{
		Hostname:     "datn0101",
		BindAddr:     "127.0.0.1:5000",
		EtcdMachines: []string{"http://localhost:4001"},
	}
	return cfg
}

func fakeChordNodeConfig() *chord.NodeConfig {
	return chord.DefaultConfig("chod0101", "serf0101")
}

func TestDataNodeOperation(t *testing.T) {
	srvCfg := fakeSrvConfig()
	nodeCfg := fakeChordNodeConfig()
	node := chord.CreateNode(nodeCfg)

	if err := RegisterDataNode(srvCfg, ecpool); err != nil {
		t.Errorf("failed to register datanode")
	}

	ec, err := ecpool.GetEtcdClient()
	if err != nil {
		t.Errorf("failed to get etcdclient: %v", err)
		return
	}
	defer ecpool.RecycleEtcdClient(ec.Id)
	c := ec.Cli

	baseKey := dmq.GetDataPNodeKey(srvCfg.Hostname)
	pubaddrKey := fmt.Sprintf("%s/%s", baseKey, dmq.DataPnodePubAddr)
	if resp, err := c.Get(pubaddrKey, false, false); err != nil {
		t.Errorf("failed to retrive pubaddrKey: %v", err)
	} else if resp.Node.Dir || resp.Node.Value != srvCfg.BindAddr {
		t.Errorf("error pubaddr result")
	}

	statusKey := fmt.Sprintf("%s/%s", baseKey, dmq.DataPnodeStatus)
	if resp, err := c.Get(statusKey, false, false); err != nil {
		t.Errorf("failed to retrive pubaddrKey: %v", err)
	} else if resp.Node.Dir || resp.Node.Value != dmq.DataNodeStatusActive {
		t.Errorf("error datanode status result")
	}

	if err := RegisterVnodes(srvCfg, node, ecpool); err != nil {
		t.Errorf("failed to RegisterVnodes: %v", err)
	}

	vnBaseKey := dmq.GetDataVnodeKey()
	for _, lvn := range node.LVnodes {
		vnk := fmt.Sprintf("%s/%s", vnBaseKey, hex.EncodeToString(lvn.Vnode.Id))
		if resp, err := c.Get(vnk, false, false); err != nil {
			t.Errorf("failed to retrive vnode hash vnk %s: %v", vnk, err)
		} else if resp.Node.Dir || resp.Node.Value != lvn.Vnode.Pnode.Hostname {
			t.Errorf("error vnode hash result")
		}
	}

	if err := UnregisterDN(srvCfg, node, ecpool); err != nil {
		t.Errorf("failed to UnregisterEtcd: %v", err)
	}
}

// FIXME: to run this function properity, we should start at least one dispatch
// node. In future work we will separate the dispatcher into a standalone module.
func TestAllocateDispNode(t *testing.T) {
	if addr, err := AllocateDispNode(ecpool); err != nil {
		// t.Errorf("failed to AllocateDispNode: %v", err)
		t.Logf("failed to allocate disp node: %v", err)
	} else {
		t.Logf("allocate disp node: %s", addr)
	}
}
