package main

import (
	"fmt"
	"testing"
)

func newCache() (*RedisCache, error) {
	cfg := MsgCacheConfig{
		Data: map[string]interface{}{
			"ConnNodeId": "conn-01-01",
			"Endpoint":   "tcp@localhost:6379",
			"MaxIdle":    50,
			"MaxActive":  100,
			"Timeout":    3600,
		},
	}
	return NewRedisCache(cfg)
}

func TestRedisCache(t *testing.T) {
	_, err := newCache()
	if err != nil {
		t.Errorf("NewRedisCache with error(%v)", err)
	}
}

func TestRedisCRUDWithRawMsg(t *testing.T) {
	var testId string = "507f191e810c19729de860ea"
	var testPayload string = "test message xiu"
	var iterRange = 1000
	rc, err := newCache()
	if err != nil {
		t.Errorf("NewRedisCache with error(%v)", err)
	}
	rmsg := &RawMsg{
		Id:      testId,
		Payload: testPayload,
		Extra:   map[string]string{},
	}
	for i := 0; i < iterRange; i++ {
		rmsg.Extra["data"] = fmt.Sprintf("%d", i)
		if err := rc.SaveRawMsg(rmsg, 30); err != nil {
			t.Errorf("SaveRawMsg with error(%v)", err)
		}
	}
	getMsg, err := rc.GetRawMsg(testId)
	if err != nil {
		t.Errorf("GetRawMsg with error(%v)", err)
	}
	if getMsg.Payload != testPayload ||
		getMsg.Extra["data"] != fmt.Sprintf("%d", (iterRange-1)) {
		t.Errorf("get message(%v) content error", getMsg)
	}
	if err := rc.DelRawMsg(testId); err != nil {
		t.Errorf("delete message error(%v)", err)
	}
	getMsgAfterDel, _ := rc.GetRawMsg(testId)
	if getMsgAfterDel != nil {
		t.Errorf("message still exists after deletetion")
	}
}
