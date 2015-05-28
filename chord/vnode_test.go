package chord

import (
	"crypto/sha1"
	"math/big"
	"testing"
)

var hashBytes = 20 // 20 * 8 = 160 bits
var vnodeNum = 16
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
	node := &Node{Config: conf}
	return &localVnode{node: node}
}

func TestVnodeInit(t *testing.T) {
	vn := makeVnode()
	vn.init(nil, vn.node.Config.StartHash)
	if len(vn.ID) != sha1.New().Size() {
		t.Errorf("wrong virtual node id %v", vn.ID)
	}
	t.Logf("virtual node id %v", vn.ID)
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

func TestHashSplit(t *testing.T) {
	start := big.NewInt(0)
	end := big.NewInt(0)
	end.Div(maxhash, big.NewInt(int64(vnodeNum)))

	bi := big.NewInt(0)
	bi.SetBytes(HashSplit(start.Bytes(), end.Bytes(), 1))
	if bi.String() != "913438523331814323877303020447676887284957839" {
		t.Errorf("error value for hash split by 1%%")
	}
	bi.SetBytes(HashSplit(start.Bytes(), end.Bytes(), 2))
	if bi.String() != "1826877046663628647754606040895353774569915678" {
		t.Errorf("error value for hash split by 2%%")
	}
	bi.SetBytes(HashSplit(start.Bytes(), end.Bytes(), 3))
	if bi.String() != "2740315569995442971631909061343030661854873518" {
		t.Errorf("error value for hash split by 3%%")
	}
	bi.SetBytes(HashSplit(start.Bytes(), end.Bytes(), 10))
	if bi.String() != "9134385233318143238773030204476768872849578393" {
		t.Errorf("error value for hash split by 10%%")
	}
	bi.SetBytes(HashSplit(start.Bytes(), end.Bytes(), 25))
	if bi.String() != "22835963083295358096932575511191922182123945984" {
		t.Errorf("error value for hash split by 25%%")
	}
	bi.SetBytes(HashSplit(start.Bytes(), end.Bytes(), 50))
	if bi.String() != "45671926166590716193865151022383844364247891968" {
		t.Errorf("error value for hash split by 50%%")
	}
	bi.SetBytes(HashSplit(start.Bytes(), end.Bytes(), 75))
	if bi.String() != "68507889249886074290797726533575766546371837952" {
		t.Errorf("error value for hash split by 75%%")
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
