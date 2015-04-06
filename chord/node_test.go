package chord

import (
	"math/big"
	"testing"
)

var conf *NodeConfig

func init() {
	conf = DefaultConfig("localhost", "serf0101")

	maxHashBytes := make([]byte, conf.HashBits/8+1)
	maxHashBytes[0] = 1
	maxhash := big.NewInt(0)
	maxhash.SetBytes(maxHashBytes)

	minusBi := big.NewInt(int64(conf.NumVnodes))
	step := big.NewInt(0)
	step.SetBytes(maxhash.Bytes())
	step.Div(step, minusBi)

	conf.maxhash = maxhash
	conf.step = step
}

func TestNodeCreate(t *testing.T) {
	node := CreateNode(conf)
	for _, vnode := range node.Vnodes {
		t.Logf("%v", vnode.Id)
	}
}
