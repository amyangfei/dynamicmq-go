package main

import (
	"fmt"
)

type IndexBase struct {
	dimension int
	attrbases []*AttrBase
}

type AttrBase struct {
	name      string
	use       int
	low, high int
	sigval    []string
}

type AttrIndex struct {
	tree  Tree
	xname string
	yname string
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

func AttrCombine(xattr, yattr string) string {
	if xattr < yattr {
		return fmt.Sprintf("%s-%s", xattr, yattr)
	} else {
		return fmt.Sprintf("%s-%s", yattr, xattr)
	}
}

func buildSigleAttrIndex(xattr, yattr *AttrBase) *AttrIndex {
	aidx := &AttrIndex{
		xname: xattr.name,
		yname: yattr.name,
		tree:  NewTree(xattr.low, xattr.high, yattr.low, yattr.high),
	}
	return aidx
}

func InitIndex(attridxes map[string]*AttrIndex, idxbase *IndexBase) error {
	c, err := GetEtcdClient(Config.EtcdMachines)
	if err != nil {
		return err
	}
	if err := LoadIndexBase(c, idxbase); err != nil {
		return err
	}

	idxNum := len(idxbase.attrbases)
	for i := 0; i < idxNum-1; i++ {
		for j := i + 1; j < idxNum; j++ {
			attrindex := buildSigleAttrIndex(
				idxbase.attrbases[i], idxbase.attrbases[j])
			ackey := AttrCombine(idxbase.attrbases[i].name, idxbase.attrbases[j].name)
			attridxes[ackey] = attrindex
		}
	}
	return nil
}
