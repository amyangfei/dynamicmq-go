package main

import (
	"fmt"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"strconv"
	"strings"
	"testing"
)

func fakeSrvConfig() *SrvConfig {
	cfg := &SrvConfig{
		NodeId:        "conn-01-01",
		SubTCPBind:    "localhost:7253",
		RouterTCPBind: "localhost:7255",
		Capacity:      12345,
		EtcdMachiens:  []string{"http://localhost:4001"},
	}
	return cfg
}

func TestRegisterEtcd(t *testing.T) {
	cfg := fakeSrvConfig()
	if err := RegisterEtcd(cfg); err != nil {
		t.Errorf("RegisterEtcd error(%v)", err)
	}
	if resp, err := GetConnInfo(cfg); err != nil {
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
					if node.Value != cfg.SubTCPBind {
						t.Errorf("value error %s != %s", node.Value, cfg.SubTCPBind)
					}
				case dmq.ConnRouteAddr:
					if node.Value != cfg.RouterTCPBind {
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
	if err := UnregisterEtcd(cfg); err != nil {
		t.Errorf("UnregisterEtcd error(%v)", err)
	}
}

func TestUpdateEtcd(t *testing.T) {
	cfg := fakeSrvConfig()

	if err := RegisterEtcd(cfg); err != nil {
		t.Errorf("RegisterEtcd error(%v)", err)
		return
	}

	c, err := GetEtcdClient(cfg)
	if err != nil {
		t.Errorf("GetEtcdClient failed(%v)", err)
		return
	}
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

	if err := UnregisterEtcd(cfg); err != nil {
		t.Errorf("UnregisterEtcd error(%v)", err)
	}
}
