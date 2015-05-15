package main

import (
	"fmt"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"strconv"
	"strings"
	"testing"
)

var ecpool, attrpool *dmq.EtcdClientPool

func init() {
	cfg := fakeSrvConfig()
	ecpool = dmq.NewEtcdClientPool(cfg.EtcdMachines, 1, 16)
	attrpool = dmq.NewEtcdClientPool(cfg.AttrEtcdMachines, 1, 16)
}

func fakeSrvConfig() *SrvConfig {
	cfg := &SrvConfig{
		NodeId:           "conn0101",
		SubTCPBind:       "localhost:7253",
		RouterTCPBind:    "localhost:7255",
		Capacity:         12345,
		EtcdMachines:     []string{"http://localhost:4001"},
		AttrEtcdMachines: []string{"http://localhost:4101"},
	}
	return cfg
}

func TestRegisterEtcd(t *testing.T) {
	cfg := fakeSrvConfig()
	if err := RegisterEtcd(cfg, ecpool); err != nil {
		t.Errorf("RegisterEtcd error(%v)", err)
	}
	if resp, err := GetConnInfo(cfg, ecpool); err != nil {
		t.Errorf("Get connector info error(%v)", err)
	} else {
		if !resp.Node.Dir {
			t.Errorf("%s should be a directory", resp.Node.Key)
		} else {
			for _, node := range resp.Node.Nodes {
				splitKey := strings.Split(node.Key, "/")
				key := splitKey[len(splitKey)-1]
				switch key {
				case dmq.ConnSubAddr:
					bind := strings.Split(cfg.SubTCPBind, ":")
					subPort := bind[len(bind)-1]
					if node.Value != fmt.Sprintf("%s:%s", cfg.BindIp, subPort) {
						t.Errorf("value error %s != %s", node.Value, cfg.SubTCPBind)
					}
				case dmq.ConnRouteAddr:
					bind := strings.Split(cfg.RouterTCPBind, ":")
					routePort := bind[len(bind)-1]
					if node.Value != fmt.Sprintf("%s:%s", cfg.BindIp, routePort) {
						t.Errorf("value error %s != %s", node.Value, cfg.RouterTCPBind)
					}
				case dmq.ConnCapacity:
					if node.Value != fmt.Sprintf("%d", cfg.Capacity) {
						t.Errorf("value error %s != %d", node.Value, cfg.Capacity)
					}
				}
			}
		}
	}
}

func TestUnregisterEtcd(t *testing.T) {
	cfg := fakeSrvConfig()
	if err := UnregisterEtcd(cfg, ecpool); err != nil {
		t.Errorf("UnregisterEtcd error(%v)", err)
	}
}

func TestUpdateEtcd(t *testing.T) {
	cfg := fakeSrvConfig()

	if err := RegisterEtcd(cfg, ecpool); err != nil {
		t.Errorf("RegisterEtcd error(%v)", err)
		return
	}

	ec, err := ecpool.GetEtcdClient()
	if err != nil {
		t.Errorf("GetEtcdClient failed(%v)", err)
		return
	}
	defer ecpool.RecycleEtcdClient(ec.Id)
	c := ec.Cli

	loadKey := fmt.Sprintf("%s/%s", dmq.GetInfoKey(dmq.EtcdConnectorType, cfg.NodeId), dmq.ConnLoad)
	for i := 0; i < 100; i++ {
		resp, err := c.Get(loadKey, false, false)
		if err != nil {
			t.Errorf("Get load failed(%v)", err)
			return
		}

		load, err := strconv.Atoi(resp.Node.Value)
		if err != nil {
			t.Errorf("convert load to int error(%v)", err)
			return
		}
		c.Update(loadKey, fmt.Sprintf("%d", load+1), 0)
	}

	if err := UnregisterEtcd(cfg, ecpool); err != nil {
		t.Errorf("UnregisterEtcd error(%v)", err)
	}
}
