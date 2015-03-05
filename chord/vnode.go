package chord

import (
	"encoding/binary"
)

func (lvn *localVnode) init(idx int) {
	lvn.genId(uint16(idx))
	cfg := lvn.node.config
	lvn.Host = cfg.Hostname
	lvn.successors = make([]*Vnode, cfg.NumSuccessors)
}

func (lvn *localVnode) genId(idx uint16) {
	conf := lvn.node.config
	hash := conf.HashFunc()
	hash.Write([]byte(conf.Hostname))
	binary.Write(hash, binary.BigEndian, idx)

	lvn.Id = hash.Sum(nil)
}
