package chord

import (
	"bytes"
	"sort"
)

// When a new physical node join the ring,
// add all of its virtual nodes into local route table
func (rt *RTable) JoinVnodes(newnodes []*Vnode) {
	for _, vn := range newnodes {
		rt.JoinVnode(vn)
	}
}

func (rt *RTable) JoinVnode(vnode *Vnode) {
	// find insertion postion
	pos := sort.Search(len(rt.vnodes), func(i int) bool {
		return bytes.Compare(rt.vnodes[i].Id, vnode.Id) >= 0
	})
	// Insert new virtual node into route-table
	if pos == len(rt.vnodes) {
		rt.vnodes = append(rt.vnodes, vnode)
	} else {
		rt.vnodes = append(rt.vnodes, nil)
		copy(rt.vnodes[pos+1:], rt.vnodes[pos:])
		rt.vnodes[pos] = vnode
	}
}

// Find the vnode who stores the key with hash of keyhash
func (rt *RTable) Search(keyhash []byte) *Vnode {
	pos := sort.Search(len(rt.vnodes), func(i int) bool {
		return bytes.Compare(rt.vnodes[i].Id, keyhash) >= 0
	})
	return rt.vnodes[pos%len(rt.vnodes)]
}

func (rt *RTable) FindPeer(hostname string) (int, *PeerNode) {
	for idx, pnode := range rt.peers {
		if pnode.Hostname == hostname {
			return idx, pnode
		}
	}
	return -1, nil
}
