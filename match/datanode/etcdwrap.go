package main

import (
	"encoding/hex"
	"fmt"
	"github.com/amyangfei/dynamicmq-go/chord"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"github.com/coreos/go-etcd/etcd"
	"math/rand"
	"strings"
)

func GetEtcdClient(machines []string) (*etcd.Client, error) {
	c := etcd.NewClient(machines)
	return c, nil
}

func RegisterDataNode(cfg *SrvConfig, pool *dmq.EtcdClientPool) error {
	ec, err := pool.GetEtcdClient()
	if err != nil {
		return err
	}
	defer pool.RecycleEtcdClient(ec.Id)
	c := ec.Cli

	baseKey := dmq.GetDataPNodeKey(cfg.Hostname)

	pubaddrKey := fmt.Sprintf("%s/%s", baseKey, dmq.DataPnodePubAddr)
	if _, err := c.Set(pubaddrKey, cfg.BindAddr, 0); err != nil {
		c.Delete(baseKey, true)
		return err
	}

	// TODO: set status active at a more accuracy time
	statusKey := fmt.Sprintf("%s/%s", baseKey, dmq.DataPnodeStatus)
	if _, err := c.Create(statusKey, dmq.DataNodeStatusActive, 0); err != nil {
		c.Delete(baseKey, true)
		return err
	}

	return nil
}

func RegisterVnodes(cfg *SrvConfig, node *chord.Node, pool *dmq.EtcdClientPool) error {
	ec, err := pool.GetEtcdClient()
	if err != nil {
		return err
	}
	defer pool.RecycleEtcdClient(ec.Id)
	c := ec.Cli

	vnBaseKey := dmq.GetDataVnodeKey()
	var idx int = -1
	var verr error = nil
	lvns := node.LVnodes
	for i, lvn := range lvns {
		vnk := fmt.Sprintf("%s/%s", vnBaseKey, hex.EncodeToString(lvn.Vnode.Id))
		if _, err := c.Create(vnk, lvn.Vnode.Pnode.Hostname, 0); err != nil {
			idx = i
			verr = err
			break
		}
	}
	// if error occurs, remove vnodes already registered.
	for i := 0; i < idx; i++ {
		lvn := lvns[i]
		vnk := fmt.Sprintf("%s/%s", vnBaseKey, hex.EncodeToString(lvn.Vnode.Id))
		c.Delete(vnk, true)
	}
	return verr
}

func UnregisterDN(cfg *SrvConfig, node *chord.Node, pool *dmq.EtcdClientPool) error {
	ec, err := pool.GetEtcdClient()
	if err != nil {
		return err
	}
	defer pool.RecycleEtcdClient(ec.Id)
	c := ec.Cli

	var reterr error = nil

	baseKey := dmq.GetDataPNodeKey(cfg.Hostname)
	if _, err := c.Delete(baseKey, true); err != nil {
		reterr = err
	}

	vnBaseKey := dmq.GetDataVnodeKey()
	for _, lvn := range node.LVnodes {
		vnk := fmt.Sprintf("%s/%s", vnBaseKey, hex.EncodeToString(lvn.Vnode.Id))
		if _, err := c.Delete(vnk, true); err != nil {
			reterr = fmt.Errorf("%v: %v", reterr, err)
		}
	}

	return reterr
}

func GetSubCliConnId(cliId string, pool *dmq.EtcdClientPool) (string, error) {
	ec, err := pool.GetEtcdClient()
	if err != nil {
		return "", err
	}
	defer pool.RecycleEtcdClient(ec.Id)
	c := ec.Cli

	subCliConnKey := dmq.GetSubConnKey(cliId)

	if resp, err := c.Get(subCliConnKey, false, false); err != nil {
		return "", err
	} else if resp.Node.Dir {
		return "", fmt.Errorf("%s should not be a directory", resp.Node.Key)
	} else {
		return resp.Node.Value, nil
	}
}

func AllocateDispNode(pool *dmq.EtcdClientPool) (*DispNode, error) {
	ec, err := pool.GetEtcdClient()
	if err != nil {
		return nil, err
	}
	defer pool.RecycleEtcdClient(ec.Id)
	c := ec.Cli

	infoKey := dmq.GetInfoBase(dmq.EtcdDispatcherType)
	resp, err := c.Get(infoKey, false, true)
	if err != nil {
		return nil, err
	}
	if !resp.Node.Dir {
		return nil, fmt.Errorf("%s is not a directory", resp.Node.Key)
	}
	dispNum := len(resp.Node.Nodes)
	if dispNum > 0 {
		idx := rand.Intn(dispNum)
		dispInfo := resp.Node.Nodes[idx]
		if !dispInfo.Dir {
			return nil, fmt.Errorf("%s is not a directory", dispInfo.Key)
		}
		dispBindKey := fmt.Sprintf("%s/%s", dispInfo.Key, dmq.DispBindAddr)
		for _, info := range dispInfo.Nodes {
			if info.Key == dispBindKey {
				dispIdKey := strings.Split(dispInfo.Key, "/")
				dispnode := &DispNode{
					dispid:   dispIdKey[len(dispIdKey)-1],
					bindAddr: info.Value,
				}
				return dispnode, nil
			}
		}
	}
	return nil, fmt.Errorf("dispatcher not found")
}
