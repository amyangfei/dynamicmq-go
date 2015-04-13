package main

import (
	"encoding/hex"
	"fmt"
	"github.com/amyangfei/dynamicmq-go/chord"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"testing"
)

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

	if err := RegisterDataNode(srvCfg); err != nil {
		t.Errorf("failed to register datanode")
	}

	c, err := GetEtcdClient(srvCfg.EtcdMachines)
	if err != nil {
		t.Errorf("failed to get etcdclient: %v", err)
	}

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

	if err := RegisterVnodes(srvCfg, node); err != nil {
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

	if err := UnregisterDN(srvCfg, node); err != nil {
		t.Errorf("failed to UnregisterEtcd: %v", err)
	}
}