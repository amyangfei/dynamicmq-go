package chord

import (
	"crypto/sha1"
	"math/big"
	"testing"
)

var hashBytes int = 20 // 20 * 8 = 160 bits
var vnodeNum int = 16
var maxhash, step *big.Int

func init() {
	maxHashBytes := make([]byte, hashBytes+1)
	maxHashBytes[0] = 1
	maxhash = big.NewInt(0)
	maxhash.SetBytes(maxHashBytes)

	minusBi := big.NewInt(int64(vnodeNum))
	step = big.NewInt(0)
	step.SetBytes(maxhash.Bytes())
	step.Div(step, minusBi)
}

func makeVnode() *localVnode {
	conf := DefaultConfig("localhost", "serf0101")
	node := &Node{config: conf}
	return &localVnode{node: node}
}

func TestVnodeInit(t *testing.T) {
	vn := makeVnode()
	vn.init(vn.node.config.startHash)
	if len(vn.Id) != sha1.New().Size() {
		t.Errorf("wrong virtual node id %v", vn.Id)
	}
	t.Logf("virtual node id %v", vn.Id)
}

func TestHashJump(t *testing.T) {
	start := make([]byte, hashBytes)
	start[hashBytes-1] = 1

	t.Logf("maxhash: %v", maxhash.Bytes())
	t.Logf("step    : %v", step.Bytes())
	t.Logf("start val: %v", start)

	newHash := start[:]
	for i := 0; i < vnodeNum+1; i++ {
		newHash = HashJump(newHash, step, maxhash)
		t.Logf("jump %d: %v", i+1, newHash)
	}
}

func BenchmarkHashJump(b *testing.B) {
	start := make([]byte, hashBytes)
	start[hashBytes-1] = 1

	newHash := start[:]
	for i := 0; i < vnodeNum+1; i++ {
		newHash = HashJump(newHash, step, maxhash)
	}
}
