package chord

import (
	"bytes"
	"sort"
)

// When a new physical node join the ring,
// add all of its virtual nodes into local route table
func (rt *RTable) Join(newnodes []*Vnode) {
	for _, vn := range newnodes {
		// find insertion postion
		pos := sort.Search(len(rt.vnodes), func(i int) bool {
			return bytes.Compare(rt.vnodes[i].Id, vn.Id) >= 0
		})
		// Insert new virtual node into route-table
		if pos == len(rt.vnodes) {
			rt.vnodes = append(rt.vnodes, vn)
		} else {
			rt.vnodes = append(rt.vnodes, nil)
			copy(rt.vnodes[pos+1:], rt.vnodes[pos:])
			rt.vnodes[pos] = vn
		}
	}
}

// Find the vnode who stores the key with hash of keyhash
func (rt *RTable) Search(keyhash []byte) *Vnode {
	pos := sort.Search(len(rt.vnodes), func(i int) bool {
		return bytes.Compare(rt.vnodes[i].Id, keyhash) >= 0
	})
	return rt.vnodes[pos%len(rt.vnodes)]
}
