package main

import (
	"fmt"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"testing"
)

var machines = []string{"http://localhost:4001"}

var dimension = 4
var names = []string{"xcoord", "ycoord", "zcoord", "time"}
var lower = []int{0, 0, 0, 0}
var upper = []int{20, 20, 20, 20}

func checkIndexBase(idxBase *IndexBase) error {
	expected := make(map[string]*AttrBase)
	for i := 0; i < dimension; i++ {
		expected[names[i]] = &AttrBase{
			name: names[i],
			use:  dmq.AttrUseField["range"],
			low:  lower[i],
			high: upper[i],
		}
	}

	for _, idx := range idxBase.attrbases {
		if expectedIdx, ok := expected[idx.name]; !ok {
			return fmt.Errorf("%s not expected", idx.name)
		} else if expectedIdx.low != idx.low || expectedIdx.high != idx.high {
			return fmt.Errorf("invalid value for %s low=%d high=%d", idx.name, idx.low, idx.high)
		}
	}
	return nil
}

func TestLoadIndexBase(t *testing.T) {
	c, _ := getEtcdClient(machines)

	idxBase := &IndexBase{}

	if err := dmq.NewAttrIndexBase(c, dimension, names, lower, upper); err != nil {
		t.Errorf("failed to create attribute index base: %v", err)
	}

	if err := loadIndexBase(c, idxBase); err != nil {
		t.Errorf("failed to load IndexBase: %v", err)
	}

	if err := checkIndexBase(idxBase); err != nil {
		t.Errorf("checkIndexBase error: %v", err)
	}
}
