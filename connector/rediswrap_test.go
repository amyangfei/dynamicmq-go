package main

import (
	"testing"
)

func testRedisConfig() *RedisConfig {
	cfg := &SrvConfig{
		NodeId:           "conn0125",
		RedisEndPoint:    "tcp@127.0.0.1:6379",
		RedisIdleTimeout: 3600,
		RedisMaxIdle:     50,
		RedisMaxActive:   100,
	}
	return NewRedisConfig(cfg)
}

func TestRedisCRUD(t *testing.T) {
	var testCliId string = "507f191e810c19729de860ea"
	rcfg := testRedisConfig()
	rc, err := NewRedisCliPool(rcfg)
	if err != nil {
		t.Errorf("NewRedisCliPool with error: %v", err)
	}

	iterTime := 100
	for i := 0; i < iterTime; i++ {
		if err := rc.RegisterSubCli(testCliId); err != nil {
			t.Errorf("failed to RegisterSubCli: %v", err)
		}
	}

	if r, err := rc.GetSubConnId(testCliId); err != nil {
		t.Errorf("failed to GetSubConn: %v", err)
	} else {
		if r != rcfg.connId {
			t.Errorf("get conn nodeid %s expected %s", r, rcfg.connId)
		}
	}

	if err := rc.UnRegisterSubCli(testCliId); err != nil {
		t.Errorf("failed to UnRegisterSubCli: %v", err)
	}
}
