package main

import ()

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

func buildSigleAttrIndex(xattr, yattr *AttrBase) *AttrIndex {
	aidx := &AttrIndex{
		xname: xattr.name,
		yname: yattr.name,
		tree:  NewTree(xattr.low, xattr.high, yattr.low, yattr.high),
	}
	return aidx
}

func InitIndex(attridxes *[]*AttrIndex, idxbase *IndexBase) error {
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
			*attridxes = append(*attridxes, attrindex)
		}
	}
	return nil
}
