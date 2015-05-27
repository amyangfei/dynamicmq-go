package main

import (
	"encoding/json"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"gopkg.in/mgo.v2/bson"
	"reflect"
	"testing"
)

var rcpool *dmq.RedisCliPool

func init() {
	rcfg := testRedisConfig()
	var err error
	rcpool, err = dmq.NewRedisCliPool(rcfg)
	if err != nil {
		panic(err)
	}
}

func testRedisConfig() *dmq.RedisConfig {
	endpoint := "tcp@127.0.0.1:6379"
	idle := 50
	active := 100
	timeout := 3600
	return dmq.NewRedisConfig(endpoint, idle, active, timeout)
}

func TestRedisCRUD(t *testing.T) {
	testConnID := "conn0123"
	testCliID := "507f191e810c19729de860ea"

	iterTime := 100
	for i := 0; i < iterTime; i++ {
		if err := registerSubCli(rcpool, testCliID, testConnID); err != nil {
			t.Errorf("failed to registerSubCli: %v", err)
		}
	}

	if r, err := dmq.GetSubConnId(rcpool, testCliID); err != nil {
		t.Errorf("failed to GetSubConn: %v", err)
	} else {
		if r != testConnID {
			t.Errorf("get conn nodeid %s expected %s", r, testConnID)
		}
	}

	if err := unRegisterSubCli(testCliID, rcpool); err != nil {
		t.Errorf("failed to unRegisterSubCli: %v", err)
	}
}

func TestAttrOper(t *testing.T) {
	testConnID := "conn0123"

	cli := &SubClient{
		id:    bson.NewObjectId(),
		attrs: make(map[string]*Attribute, 0),
	}
	attr := &Attribute{
		name:   "test_attr",
		use:    byte(dmq.AttrUseField["strval"]),
		strval: "test app",
	}
	cli.attrs[attr.name] = attr
	attrKey := dmq.GetSubAttrKey(cli.id.Hex(), attr.name)

	if err := registerSubCli(rcpool, cli.id.Hex(), testConnID); err != nil {
		t.Errorf("Failed to register subclient: %v", err)
	}

	if err := createSubAttr(cli, attr, rcpool); err != nil {
		t.Errorf("Failed to create subscriber attribute %v: %v", attr, err)
	}

	if err := UpdateSubAttr(cli, attr, rcpool); err != nil {
		t.Errorf("Failed to update subscriber attribute %v: %v", attr, err)
	}

	if resp, err := getSubAttr(cli, attr.name, rcpool); err != nil {
		t.Errorf("Failed to get subscriber attribute: %s", attrKey)
	} else {
		jsonData := make(map[string]interface{})
		if err := json.Unmarshal([]byte(resp), &jsonData); err != nil {
			t.Errorf("Failed to Unmarshal: %s", resp)
		}

		if use, ok := jsonData["use"].(float64); !ok {
			t.Errorf("use field with error type: %v", reflect.TypeOf(jsonData["use"]))
		} else {
			if floatCompare(use, float64(attr.use)) != 0 {
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
	cli.attrs[attr.name] = attr
	if err := createSubAttr(cli, attr, rcpool); err != nil {
		t.Errorf("Failed to create subscriber attribute %v: %v", attr, err)
	}
	if err := UpdateSubAttr(cli, attr, rcpool); err != nil {
		t.Errorf("Failed to update subscriber attribute: %v", attr)
	}
	if resp, err := getSubAttr(cli, attr.name, rcpool); err != nil {
		t.Errorf("Failed to get subscriber attribute: %s", attrKey)
	} else {
		jsonData := make(map[string]interface{})
		if err := json.Unmarshal([]byte(resp), &jsonData); err != nil {
			t.Errorf("Failed to Unmarshal: %s", resp)
		}

		if use, ok := jsonData["use"].(float64); !ok {
			t.Errorf("use field with error type: %v", reflect.TypeOf(jsonData["use"]))
		} else {
			if floatCompare(use, float64(attr.use)) != 0 {
				t.Errorf("use field doesn't equal to original: %f", use)
			}
		}

		if low, ok := jsonData["low"].(float64); !ok {
			t.Errorf("low field with error type: %v", reflect.TypeOf(jsonData["low"]))
		} else if floatCompare(low, attr.low) != 0 {
			t.Errorf("low field doesn't equal to original: %f", low)
		}
	}

	removeSubAttrs(cli, rcpool)

	if err := unRegisterSubCli(cli.id.Hex(), rcpool); err != nil {
		t.Errorf("failed to unRegisterSubCli: %v", err)
	}
}
