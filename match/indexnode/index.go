package main

import (
	"bytes"
	"encoding/hex"
	"fmt"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"math"
	"sort"
	"time"
)

// IndexBase stores all attribute AttrBase information
type IndexBase struct {
	dimension int
	// mapping from attribute name to this attribute's information(AttrBase)
	attrbases map[string]*AttrBase
}

// AttrBase represents an attribute boundary in index space
type AttrBase struct {
	name      string
	use       int
	low, high int
	sigval    []string
}

// AttrIndex represents a standalone segment tree index structure,
// including two dimension's attribute.
type AttrIndex struct {
	// True if this attrindex is being or to be updated
	updating bool

	// Mapping from id of subcli with unflushed attribute to its SubCliInfo
	// subcli id is BSON format string, not hex string.
	// TODO: access to this directory should be protected with RWLock
	waitUpdate map[string]*SubCliInfo

	tree  Tree
	xname string
	yname string
}

// Attribute struct
// TODO: uniform Attribute structure, also used in connector module.
type Attribute struct {
	name   string
	use    byte
	strval string
	low    float64
	high   float64
	extra  string
}

// SubCliInfo stores a subscriber's information
type SubCliInfo struct {
	Cid     []byte               // subscribe client's Id
	CidHash []byte               // cid's hash in datanode
	Attrs   []*Attribute         // subscription attribute array
	Intval  map[string]*Interval // mapping from attr-combine name to interval stores in segment tree
}

func attrNameLess(xattr, yattr string) bool {
	return xattr < yattr
}

func attrNameCombine(xattr, yattr string) string {
	if xattr < yattr {
		return fmt.Sprintf("%s-%s", xattr, yattr)
	}
	return fmt.Sprintf("%s-%s", yattr, xattr)
}

func attrSort(xattr, yattr *Attribute) (*Attribute, *Attribute) {
	if xattr.name < yattr.name {
		return xattr, yattr
	}
	return yattr, xattr
}

func attrBaseSort(xab, yab *AttrBase) (*AttrBase, *AttrBase) {
	if xab.name < yab.name {
		return xab, yab
	}
	return yab, xab
}

func buildSigleAttrIndex(xattr, yattr *AttrBase) *AttrIndex {
	aidx := &AttrIndex{
		updating:   false,
		waitUpdate: make(map[string]*SubCliInfo),
		xname:      xattr.name,
		yname:      yattr.name,
		tree:       NewTree(xattr.low, xattr.high, yattr.low, yattr.high),
	}
	return aidx
}

func initIndex(attridxes map[string]*AttrIndex, idxbase *IndexBase, pool *dmq.EtcdClientPool) error {
	ec, err := pool.GetEtcdClient()
	if err != nil {
		return err
	}
	defer pool.RecycleEtcdClient(ec.Id)
	c := ec.Cli

	if err := loadIndexBase(c, idxbase); err != nil {
		return err
	}

	var attrbasesArray []*AttrBase
	for _, ab := range idxbase.attrbases {
		attrbasesArray = append(attrbasesArray, ab)
	}
	idxNum := len(attrbasesArray)
	for i := 0; i < idxNum-1; i++ {
		for j := i + 1; j < idxNum; j++ {
			attrindex := buildSigleAttrIndex(
				attrBaseSort(attrbasesArray[i], attrbasesArray[j]))
			ackey := attrNameCombine(
				attrbasesArray[i].name, attrbasesArray[j].name)
			attridxes[ackey] = attrindex
		}
	}
	return nil
}

func (aidx *AttrIndex) insertCliAttr(xmin, xmax, ymin, ymax int, data *[]byte) *Interval {
	return aidx.tree.Push(xmin, xmax, ymin, ymax, data)
}

func compareCid(cid1, cid2 string) int {
	return bytes.Compare([]byte(cid1), []byte(cid2))
}

func (aidx *AttrIndex) insertWaitUpdate(cid string) {
	// FIXME: we can't modify waitUpdate while the aidx is processing attribute
	// flush
	go func() {
		for aidx.updating {
			time.Sleep(time.Millisecond * 10)
		}
		if _, ok := aidx.waitUpdate[cid]; !ok {
			scInfo := ClisInfo[cid]
			if scInfo == nil {
				log.Error("SubCliInfo of %s not found in ClisInfo",
					hex.EncodeToString([]byte(cid)))
				return
			}
			aidx.waitUpdate[cid] = scInfo
		}
	}()
}

// If subcli's id is in aidx's waitUpdate, check the subcli's new attribute
// whether covers the given msg, if covered return true, else return false. If
// subcli's id is not in aidx's waitUpdate, return true directly.
func (aidx *AttrIndex) filterSubCli(cid string, pubmsg *PubMsg) bool {
	scInfo, ok := aidx.waitUpdate[cid]
	if !ok {
		// cid is not in aidx's waitUpdate, return true directly
		return true
	}
	cliAttrMap := make(map[string]*Attribute)
	for _, attr := range scInfo.Attrs {
		cliAttrMap[attr.name] = attr
	}
	for _, pmsgAttr := range pubmsg.attrs {
		// find attribute of subcli with name of pmsgAttr.name
		if cliAttr, ok := cliAttrMap[pmsgAttr.name]; ok {
			if int(cliAttr.use) == dmq.AttrUseField[dmq.AttrUseRange] {
				if cliAttr.high < pmsgAttr.val || cliAttr.low > pmsgAttr.val {
					return false
				}
			}
		}
	}

	return true
}

func attrRangeFilter(xattr, yattr *Attribute) (xmin, xmax, ymin, ymax int) {
	xmin, xmax = int(math.Floor(xattr.low)), int(math.Ceil(xattr.high))
	ymin, ymax = int(math.Floor(yattr.low)), int(math.Ceil(yattr.high))
	if xmin < IdxBase.attrbases[xattr.name].low {
		xmin = IdxBase.attrbases[xattr.name].low
	}
	if xmax > IdxBase.attrbases[xattr.name].high {
		xmax = IdxBase.attrbases[xattr.name].high
	}
	if ymin < IdxBase.attrbases[yattr.name].low {
		ymin = IdxBase.attrbases[yattr.name].low
	}
	if ymax > IdxBase.attrbases[yattr.name].high {
		ymax = IdxBase.attrbases[yattr.name].high
	}
	return
}

func (aidx *AttrIndex) flushAttrUpdate() {
	cname := attrNameCombine(aidx.xname, aidx.yname)
	waitCount := len(aidx.waitUpdate)
	for cid, scInfo := range aidx.waitUpdate {
		var xattr, yattr *Attribute
		for _, subattr := range scInfo.Attrs {
			if subattr.name == aidx.xname {
				xattr = subattr
			}
			if subattr.name == aidx.yname {
				yattr = subattr
			}
		}

		ival := scInfo.Intval[cname]
		if ival != nil {
			aidx.tree.Delete(ival)
			delete(scInfo.Intval, cname)
		}

		// Sub client may have already offline before attribute flush
		if xattr != nil && yattr != nil {
			xmin, xmax, ymin, ymax := attrRangeFilter(xattr, yattr)

			newival := aidx.insertCliAttr(xmin, xmax, ymin, ymax, &ClisInfo[cid].Cid)
			ClisInfo[cid].Intval[cname] = newival
		}

		delete(aidx.waitUpdate, cid)
	}
	aidx.updating = false
	log.Debug("Flush %d attribute for index %s-%s", waitCount, aidx.xname, aidx.yname)
}

func processAttrUpdateFlush(lastUpdate int64) int64 {
	if len(AttrIdxesMap) == 0 {
		log.Error("length of AttrIdxesMap should not be zero")
		return lastUpdate
	}

	updateTs := lastUpdate
	// Flush attribute update for each AttrIndex if its updating field is set
	// true in the last process round.
	for _, aidx := range AttrIdxesMap {
		if aidx.updating {
			aidx.flushAttrUpdate()
			updateTs = time.Now().Unix()
		}
	}

	// Sort AttrIndex by length of their waitUpdate in descending order
	var sortAttrIdxes []*AttrIndex
	for _, aidx := range AttrIdxesMap {
		pos := sort.Search(len(sortAttrIdxes), func(i int) bool {
			return len(sortAttrIdxes[i].waitUpdate) <= len(aidx.waitUpdate)
		})
		if pos == len(sortAttrIdxes) {
			sortAttrIdxes = append(sortAttrIdxes, aidx)
		} else {
			sortAttrIdxes = append(sortAttrIdxes, nil)
			copy(sortAttrIdxes[pos+1:], sortAttrIdxes[pos:])
			sortAttrIdxes[pos] = aidx
		}
	}

	// If no AttrIndex's waitUpdate reaches UpdateCache threshold, and the
	// system hasn't processing attribute flush longger than FlushTimeout,
	// force the AttrIndex with largest UpdateCache to process flush next round.
	if len(sortAttrIdxes[0].waitUpdate) < Config.UpdateCacheThr &&
		len(sortAttrIdxes[0].waitUpdate) > 0 &&
		updateTs+int64(Config.FlushTimeout) < time.Now().Unix() {
		sortAttrIdxes[0].updating = true
		return updateTs
	}

	for i := 0; i < len(sortAttrIdxes)/2; i++ {
		if len(sortAttrIdxes[i].waitUpdate) < Config.UpdateCacheThr {
			return updateTs
		}
		sortAttrIdxes[i].updating = true
	}

	return updateTs
}

func (scInfo *SubCliInfo) attrUpdate(attr *Attribute) {
	hasUpdate := false
	for _, oldAttr := range scInfo.Attrs {
		if oldAttr.name == attr.name && oldAttr.use == attr.use {
			if oldAttr.low != attr.low {
				oldAttr.low = attr.low
				hasUpdate = true
			}
			if oldAttr.high != attr.high {
				oldAttr.high = attr.high
				hasUpdate = true
			}
			break
		}
	}

	if hasUpdate {
		// find all attribute name combination
		for _, existAttr := range scInfo.Attrs {
			if existAttr.name != attr.name {
				cname := attrNameCombine(existAttr.name, attr.name)
				if aidx, ok := AttrIdxesMap[cname]; !ok {
					log.Error(
						"cname %s not found in AttrIdxesMap for subcli %s",
						cname, hex.EncodeToString(scInfo.Cid))
				} else {
					aidx.insertWaitUpdate(string(scInfo.Cid))
				}
			}
		}
	}
}
