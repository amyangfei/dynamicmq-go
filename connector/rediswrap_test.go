package main

import (
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"testing"
)

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
	rcfg := testRedisConfig()
	rc, err := dmq.NewRedisCliPool(rcfg)
	if err != nil {
		t.Errorf("NewRedisCliPool with error: %v", err)
	}

	iterTime := 100
	for i := 0; i < iterTime; i++ {
		if err := RegisterSubCli(rc, testCliId, testConnId); err != nil {
			t.Errorf("failed to RegisterSubCli: %v", err)
		}
	}

	if r, err := dmq.GetSubConnId(rc, testCliId); err != nil {
		t.Errorf("failed to GetSubConn: %v", err)
	} else {
		if r != testConnId {
			t.Errorf("get conn nodeid %s expected %s", r, testConnId)
		}
	}

	if err := UnRegisterSubCli(rc, testCliId); err != nil {
		t.Errorf("failed to UnRegisterSubCli: %v", err)
	}
}
