package chord

import (
	"crypto/sha1"
	"testing"
)

func makeVnode() *localVnode {
	conf := &NodeConfig{
		NumSuccessors: 8,
		HashFunc:      sha1.New,
	}
	node := &Node{config: conf}
	return &localVnode{node: node}
}

func TestVnodeInit(t *testing.T) {
	vn := makeVnode()
	vn.init(0)
	t.Logf("genid: %x %d %d", vn.Id, len(vn.Id), sha1.New().Size())
}
