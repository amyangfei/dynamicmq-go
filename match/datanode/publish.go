package main

import ()

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
	Cid     []byte       // subscribe client's Id
	CidHash []byte       // cid's hash in datanode
	Attrs   []*Attribute // subscription attribute array
}
