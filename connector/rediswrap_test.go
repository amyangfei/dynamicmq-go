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
	var testConnId string = "conn0123"
	var testCliId string = "507f191e810c19729de860ea"

	iterTime := 100
	for i := 0; i < iterTime; i++ {
		if err := RegisterSubCli(rcpool, testCliId, testConnId); err != nil {
			t.Errorf("failed to RegisterSubCli: %v", err)
		}
	}

	if r, err := dmq.GetSubConnId(rcpool, testCliId); err != nil {
		t.Errorf("failed to GetSubConn: %v", err)
	} else {
		if r != testConnId {
			t.Errorf("get conn nodeid %s expected %s", r, testConnId)
		}
	}

	if err := UnRegisterSubCli(testCliId, rcpool); err != nil {
		t.Errorf("failed to UnRegisterSubCli: %v", err)
	}
}

func TestAttrOper(t *testing.T) {
	var testConnId string = "conn0123"

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

	if err := RegisterSubCli(rcpool, cli.id.Hex(), testConnId); err != nil {
		t.Errorf("Failed to register subclient: %v", err)
	}

	if err := CreateSubAttr(cli, attr, rcpool); err != nil {
		t.Errorf("Failed to create subscriber attribute %v: %v", attr, err)
	}

	if err := UpdateSubAttr(cli, attr, rcpool); err != nil {
		t.Errorf("Failed to update subscriber attribute %v: %v", attr, err)
	}

	if resp, err := GetSubAttr(cli, attr.name, rcpool); err != nil {
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
	cli.attrs[attr.name] = attr
	if err := CreateSubAttr(cli, attr, rcpool); err != nil {
		t.Errorf("Failed to create subscriber attribute %v: %v", attr, err)
	}
	if err := UpdateSubAttr(cli, attr, rcpool); err != nil {
		t.Errorf("Failed to update subscriber attribute: %v", attr)
	}
	if resp, err := GetSubAttr(cli, attr.name, rcpool); err != nil {
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

	RemoveSubAttrs(cli, rcpool)

	if err := UnRegisterSubCli(cli.id.Hex(), rcpool); err != nil {
		t.Errorf("failed to UnRegisterSubCli: %v", err)
	}
}
