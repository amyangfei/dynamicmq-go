package main

import (
	"bytes"
	"fmt"
	"net"
	"sort"
)

// physical node in datanode cluster
type Pnode struct {
	id       string
	bindAddr string
	vnum     int // vnode number
}

// virtual node in datanode cluster
type Vnode struct {
	id []byte // virtual node id
	pn *Pnode
}

type RTable struct {
	vns []*Vnode
}

type DnodeConn struct {
	conn net.Conn
}

func compareVid(vid1, vid2 []byte) int {
	return bytes.Compare(vid1, vid2)
}

// if allowSame is false, refuse to insert into a vnode if the rtable exists
// a vnode with the same id.
// return true if vnode is inserted into rtable's vns.
func (rt *RTable) JoinVnode(vn *Vnode, allowSame bool) bool {
	// find insertion postion
	pos := sort.Search(len(rt.vns), func(i int) bool {
		return compareVid(rt.vns[i].id, vn.id) >= 0
	})

	if !allowSame {
		if pos < len(rt.vns) && compareVid(rt.vns[pos].id, vn.id) == 0 {
			return false
		}
	}

	// Insert new virtual node into route-table
	if pos == len(rt.vns) {
		rt.vns = append(rt.vns, vn)
	} else {
		rt.vns = append(rt.vns, nil)
		copy(rt.vns[pos+1:], rt.vns[pos:])
		rt.vns[pos] = vn
	}
	return true
}

// Find the vnode with id of keyhash
func (rt *RTable) Search(keyhash []byte) (int, *Vnode) {
	pos := sort.Search(len(rt.vns), func(i int) bool {
		return compareVid(rt.vns[i].id, keyhash) >= 0
	})
	if pos < len(rt.vns) && compareVid(rt.vns[pos].id, keyhash) == 0 {
		return pos, rt.vns[pos]
	} else {
		return -1, nil
	}
}

// Find the vnode who stores the key with hash of keyhash
func (rt *RTable) StoreSearch(keyhash []byte) *Vnode {
	if len(rt.vns) == 0 {
		return nil
	}
	pos := sort.Search(len(rt.vns), func(i int) bool {
		return compareVid(rt.vns[i].id, keyhash) >= 0
	})
	return rt.vns[pos%len(rt.vns)]
}

func buildDnodeConn(nid string) (net.Conn, error) {
	pn, pok := PnodeMap[nid]
	if !pok {
		return nil, fmt.Errorf("pnode with id=%s not found", nid)
	}
	addr, err := net.ResolveTCPAddr("tcp", pn.bindAddr)
	if err != nil {
		return nil, err
	}
	return net.DialTCP("tcp", nil, addr)
}

func getDnodeConn(nid string) (*DnodeConn, error) {
	dnconn, ok := DnConns[nid]
	if !ok {
		conn, err := buildDnodeConn(nid)
		if err != nil {
			return nil, err
		}
		// TODO: start connection heartbeat to datanode here.

		dnconn = &DnodeConn{
			conn: conn,
		}
		DnConns[nid] = dnconn
	}
	return dnconn, nil
}

func (dnconn *DnodeConn) WriteMsg(msg []byte) (int, error) {
	// TODO: error handling. e.g. broken connection, write failed etc.
	return dnconn.conn.Write(msg)
}
