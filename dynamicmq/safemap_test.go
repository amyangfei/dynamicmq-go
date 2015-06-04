package dynamicmq

import (
	"testing"
)

func TestConcurrentMap(t *testing.T) {
	bm := NewConcurrentMap()
	if !bm.Set("dfltKey", 1) {
		t.Error("set Error")
	}

	if !bm.Haskey("dfltKey") {
		t.Error("haskey err")
	}

	if v := bm.Get("dfltKey"); v.(int) != 1 {
		t.Error("get err")
	}
	if v := bm.Get("notExistsKey"); v != nil {
		t.Error("get not exists key error")
	}

	bm.Delete("dfltKey")
	if bm.Haskey("dfltKey") {
		t.Error("delete err")
	}
}
