package chord

import (
	"math/big"
)

func (lvn *localVnode) init(pnode *PeerNode, curHash []byte) {
	lvn.Id = curHash[:]
	lvn.Pnode = pnode
	lvn.successors = make([]*Vnode, lvn.node.Config.NumSuccessors)
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

func HashSplit(start, end []byte, percent int) []byte {
	if percent < 1 || percent > 99 {
		percent = 50
	}

	biStart := big.NewInt(0)
	biStart.SetBytes(start)
	biEnd := big.NewInt(0)
	biEnd.SetBytes(end)
	biSplit := big.NewInt(0)

	if percent == 50 {
		biSplit.Sub(biEnd, biStart)
		biSplit.Div(biSplit, big.NewInt(2))
		biSplit.Add(biStart, biSplit)
	} else {
		biSplit.Sub(biEnd, biStart)
		biSplit.Mul(biSplit, big.NewInt(int64(percent)))
		biSplit.Div(biSplit, big.NewInt(100))
		biSplit.Add(biStart, biSplit)
	}
	return biSplit.Bytes()
}
