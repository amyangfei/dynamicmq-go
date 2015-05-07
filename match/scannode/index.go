package main

import (
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"sync"
)

type IndexBase struct {
	dimension int
	// mapping from attribute name to this attribute's information(AttrBase)
	attrbases map[string]*AttrBase
}

type AttrBase struct {
	name      string
	use       int
	low, high int
	sigval    []string
}

// TODO: uniform Attribute structure, also used in connector module.
type Attribute struct {
	name   string
	use    byte
	strval string
	low    float64
	high   float64
	extra  string
}

type SubCliInfo struct {
	lock    *sync.RWMutex
	Cid     []byte       // subscribe client's Id
	CidHash []byte       // cid's hash in datanode
	Attrs   []*Attribute // subscription attribute array
}

func InitIndex(idxbase *IndexBase, pool *dmq.EtcdClientPool) error {
	ec, err := pool.GetEtcdClient()
	if err != nil {
		return err
	}
	defer pool.RecycleEtcdClient(ec.Id)
	c := ec.Cli

	if err := LoadIndexBase(c, idxbase); err != nil {
		return err
	}

	return nil
}
