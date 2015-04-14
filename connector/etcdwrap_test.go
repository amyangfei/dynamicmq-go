package main

import (
	"encoding/json"
	"fmt"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"gopkg.in/mgo.v2/bson"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

var ecpool *dmq.EtcdClientPool

func init() {
	cfg := fakeSrvConfig()
	ecpool = dmq.NewEtcdClientPool(cfg.EtcdMachines, 1, 16)
}

func fakeSrvConfig() *SrvConfig {
	cfg := &SrvConfig{
		NodeId:        "conn0101",
		SubTCPBind:    "localhost:7253",
		RouterTCPBind: "localhost:7255",
		Capacity:      12345,
		EtcdMachines:  []string{"http://localhost:4001"},
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

func TestUpdateAttr(t *testing.T) {
	cfg := fakeSrvConfig()
	cli := &SubClient{
		id: bson.NewObjectId(),
	}
	attr := &Attribute{
		name:   "test_attr",
		use:    byte(dmq.AttrUseField["strval"]),
		strval: "test app",
	}
	attrKey := dmq.GetSubAttrKey(cli.id.Hex(), attr.name)

	if err := RegisterSub(cli, cfg, ecpool); err != nil {
		t.Errorf("Failed to register subclient: %v", err)
	}

	if err := CreateSubAttr(cli, attr, cfg, ecpool); err != nil {
		t.Errorf("Failed to create subscriber attribute %v: %v", attr, err)
	}

	if err := UpdateSubAttr(cli, attr, cfg, ecpool); err != nil {
		t.Errorf("Failed to update subscriber attribute %v: %v", attr, err)
	}

	if resp, err := GetSubAttr(cli, attr.name, cfg, ecpool); err != nil {
		t.Errorf("Failed to get subscriber attribute: %s", attrKey)
	} else {
		jsonData := make(map[string]interface{})
		if err := json.Unmarshal([]byte(resp), &jsonData); err != nil {
			t.Errorf("Failed to Unmarshal: %s", resp)
		}

		if use, ok := jsonData["use"].(float64); !ok {
			t.Errorf("use field with error type: %v", reflect.TypeOf(jsonData["use"]))
		} else {
			if FloatCompare(use, float64(attr.use)) != 0 {
				t.Errorf("use field doesn't equal to original: %f", use)
			}
		}

		if jsonData["strval"] != attr.strval {
			t.Errorf("strval field doesn't equal to original: %s", jsonData["strval"])
		}
	}

	attr = &Attribute{
		name: "range_test",
		use:  byte(dmq.AttrUseField["range"]),
		low:  12.3,
		high: 21.7,
	}
	if err := CreateSubAttr(cli, attr, cfg, ecpool); err != nil {
		t.Errorf("Failed to create subscriber attribute %v: %v", attr, err)
	}
	if err := UpdateSubAttr(cli, attr, cfg, ecpool); err != nil {
		t.Errorf("Failed to update subscriber attribute: %v", attr)
	}
	if resp, err := GetSubAttr(cli, attr.name, cfg, ecpool); err != nil {
		t.Errorf("Failed to get subscriber attribute: %s", attrKey)
	} else {
		jsonData := make(map[string]interface{})
		if err := json.Unmarshal([]byte(resp), &jsonData); err != nil {
			t.Errorf("Failed to Unmarshal: %s", resp)
		}

		if use, ok := jsonData["use"].(float64); !ok {
			t.Errorf("use field with error type: %v", reflect.TypeOf(jsonData["use"]))
		} else {
			if FloatCompare(use, float64(attr.use)) != 0 {
				t.Errorf("use field doesn't equal to original: %f", use)
			}
		}

		if low, ok := jsonData["low"].(float64); !ok {
			t.Errorf("low field with error type: %v", reflect.TypeOf(jsonData["low"]))
		} else if FloatCompare(low, attr.low) != 0 {
			t.Errorf("low field doesn't equal to original: %f", low)
		}
	}

	RemoveSub(cli, cfg, ecpool)
}
