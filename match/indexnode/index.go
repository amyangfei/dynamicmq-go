package main

import (
	"fmt"
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

// A standalone segment tree index structure, including two dimension's attribute.
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

type SubCliInfo struct {
	Cid    []byte               // subscribe client's Id
	Attrs  []*Attribute         // subscription attribute array
	Intval map[string]*Interval // mapping from attr-combine name to interval stores in segment tree
}

func AttrNameCombine(xattr, yattr string) string {
	if xattr < yattr {
		return fmt.Sprintf("%s-%s", xattr, yattr)
	} else {
		return fmt.Sprintf("%s-%s", yattr, xattr)
	}
}

func AttrSort(xattr, yattr *Attribute) (*Attribute, *Attribute) {
	if xattr.name < yattr.name {
		return xattr, yattr
	} else {
		return yattr, xattr
	}
}

func AttrBaseSort(xab, yab *AttrBase) (*AttrBase, *AttrBase) {
	if xab.name < yab.name {
		return xab, yab
	} else {
		return yab, xab
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

	attrbasesArray := make([]*AttrBase, 0)
	for _, ab := range idxbase.attrbases {
		attrbasesArray = append(attrbasesArray, ab)
	}
	idxNum := len(attrbasesArray)
	for i := 0; i < idxNum-1; i++ {
		for j := i + 1; j < idxNum; j++ {
			attrindex := buildSigleAttrIndex(
				AttrBaseSort(attrbasesArray[i], attrbasesArray[j]))
			ackey := AttrNameCombine(
				attrbasesArray[i].name, attrbasesArray[j].name)
			attridxes[ackey] = attrindex
		}
	}
	return nil
}

func (aidx *AttrIndex) InsertCliAttr(xmin, xmax, ymin, ymax int, data *[]byte) *Interval {
	return aidx.tree.Push(xmin, xmax, ymin, ymax, data)
}
