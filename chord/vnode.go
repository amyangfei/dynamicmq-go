package chord

import (
	"math/big"
)

func (lvn *localVnode) init(curHash []byte) {
	// lvn.Id = make([]byte, len(curHash))
	lvn.Id = curHash[:]
	lvn.Host = lvn.node.config.Hostname
	lvn.successors = make([]*Vnode, lvn.node.config.NumSuccessors)
}

func HashJump(start []byte, step, maxhash *big.Int) []byte {
	bi := big.NewInt(0)
	bi.SetBytes(start)
	bi.Add(bi, step)

	if bi.Cmp(maxhash) > 0 {
		bi.Sub(bi, maxhash)
	}
	return bi.Bytes()
}
