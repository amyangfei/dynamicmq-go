package dynamicmq

// Derivatived from https://github.com/Terry-Mao/gopush-cluster

import (
	"bufio"
	"io"
)

// TcpBufCache struct
type TcpBufCache struct {
	instanceNum int
	instance    []chan *bufio.Reader
	round       int
}

// NewTcpBufCache returns a new TcpBuf cache.
func NewTcpBufCache(instanceNum, bufioNum int) *TcpBufCache {
	inst := make([]chan *bufio.Reader, instanceNum)
	for i := 0; i < instanceNum; i++ {
		inst[i] = make(chan *bufio.Reader, bufioNum)
	}
	return &TcpBufCache{instanceNum: instanceNum, instance: inst, round: 0}
}

// Get returns a chan bufio.Reader (in round-robin fashion).
func (b *TcpBufCache) Get() chan *bufio.Reader {
	rc := b.instance[b.round]
	// split requets to different buffer chan
	if b.round++; b.round == b.instanceNum {
		b.round = 0
	}
	return rc
}

// NewBufioReader tries to get a Reader by chan,
// if chan is empty then allocates a new Reader.
func NewBufioReader(c chan *bufio.Reader, r io.Reader, bufsz int) *bufio.Reader {
	select {
	case p := <-c:
		p.Reset(r)
		return p
	default:
		return bufio.NewReaderSize(r, bufsz)
	}
}

// RecycleBufioReader recycles a Reader back to chan, if chan full discard it.
func RecycleBufioReader(c chan *bufio.Reader, r *bufio.Reader) {
	r.Reset(nil)
	select {
	case c <- r:
	default:
		// discard it
	}
}
