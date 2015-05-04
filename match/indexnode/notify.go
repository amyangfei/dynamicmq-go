package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"github.com/coreos/go-etcd/etcd"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	AttrCmdTable = map[string]func(data *etcd.Response) error{
		dmq.EtcdActionCreate: processAttrCreate,
		dmq.EtcdActionUpdate: processAttrUpdate,
		dmq.EtcdActionDelete: processAttrDelete,
	}

	DataNodeCmdTable = map[string]func(data *etcd.Response) error{
		dmq.EtcdActionCreate: processDataNodeCreate,
		dmq.EtcdActionUpdate: processDataNodeUpdate,
		dmq.EtcdActionDelete: processDataNodeDelete,
	}
)

func extractAttrFromSubVal(subval string) (*Attribute, error) {
	attrData := make(map[string]interface{})
	if err := json.Unmarshal([]byte(subval), &attrData); err != nil {
		return nil, err
	}

	use, ok := attrData["use"]
	if !ok {
		return nil, fmt.Errorf("invalid sub record, use field not found")
	}
	useval, ok := use.(float64)
	if !ok {
		return nil, fmt.Errorf("invalid sub record, use field should be numeric")
	}

	attr := &Attribute{use: byte(useval)}

	switch int(useval) {
	case dmq.AttrUseField["strval"]:
		if strval, ok := attrData["strval"].(string); !ok {
			return nil, fmt.Errorf("invalid sub record, strval not found")
		} else {
			attr.strval = strval
		}
	case dmq.AttrUseField["range"]:
		low, ok := attrData["low"].(float64)
		if !ok {
			return nil, fmt.Errorf("invalid sub record, low not found")
		}
		high, ok := attrData["high"].(float64)
		if !ok {
			return nil, fmt.Errorf("invalid sub record, high not found")
		}
		if high < low {
			return nil, fmt.Errorf("invalid attribute %f < %f", high, low)
		}
		attr.low, attr.high = low, high
	case dmq.AttrUseField["extra"]:
		if extra, ok := attrData["extra"].(string); !ok {
			return nil, fmt.Errorf("invalid sub record, extra not found")
		} else {
			attr.extra = extra
		}
	default:
		return nil, fmt.Errorf("invalid use field: %s", use)
	}

	return attr, nil
}

func processAttrCreate(data *etcd.Response) error {
	cliId, attrName := dmq.ExtractInfoFromSubKey(data.Node.Key)
	if cliId == "" || attrName == "" {
		return fmt.Errorf("invalid attr craete notify key: %s", data.Node.Key)
	}
	attr, err := extractAttrFromSubVal(data.Node.Value)
	if err != nil {
		return err
	}
	attr.name = attrName

	// TODO: support more attribute expression besides 'range'
	if int(attr.use) != dmq.AttrUseField["range"] {
		return nil
	}

	cid, err := hex.DecodeString(cliId)
	if err != nil {
		return fmt.Errorf("invalid client id: %s", cliId)
	}
	cidstr := string(cid)
	if _, ok := ClisInfo[cidstr]; !ok {
		ClisInfo[cidstr] = &SubCliInfo{
			Cid:     cid,
			CidHash: dmq.GenHash(cid, Config.HashFunc),
			Attrs:   make([]*Attribute, 0),
			Intval:  make(map[string]*Interval),
		}
		ClisInfo[cidstr].Attrs = append(ClisInfo[cidstr].Attrs, attr)
	} else {
		// Iterate each attribute and make up a new attribute pair with the new
		// attrbute and insert it into corresponding AttrIndex.
		for _, subattr := range ClisInfo[cidstr].Attrs {
			xattr, yattr := AttrSort(subattr, attr)
			cname := AttrNameCombine(xattr.name, yattr.name)
			if aidx, ok := AttrIdxesMap[cname]; !ok {
				return fmt.Errorf("attribute combine name %s not found", cname)
			} else {
				xmin, xmax, ymin, ymax := attrRangeFilter(xattr, yattr)
				ival := aidx.InsertCliAttr(
					xmin, xmax, ymin, ymax, &ClisInfo[cidstr].Cid)
				ClisInfo[cidstr].Intval[cname] = ival
			}

			log.Debug("attr combine %s-%s", xattr.name, yattr.name)
		}
		ClisInfo[cidstr].Attrs = append(ClisInfo[cidstr].Attrs, attr)
	}

	return nil
}

func processAttrUpdate(data *etcd.Response) error {
	cliId, attrName := dmq.ExtractInfoFromSubKey(data.Node.Key)
	if cliId == "" || attrName == "" {
		return fmt.Errorf("invalid attr craete notify key: %s", data.Node.Key)
	}
	attr, err := extractAttrFromSubVal(data.Node.Value)
	if err != nil {
		return err
	}
	attr.name = attrName

	if int(attr.use) == dmq.AttrUseField[dmq.AttrUseStr] {
		if ts, err := strconv.ParseInt(attr.strval, 10, 0); err != nil {
			return err
		} else {
			now := time.Now().UnixNano()
			latency := now - ts
			log.Debug("recv attr strval update with latency: %d", latency)
		}
		return nil
	}

	// TODO: support more attribute expression besides 'range'
	if int(attr.use) != dmq.AttrUseField[dmq.AttrUseRange] {
		return nil
	}

	hasUpdate := false
	cid, err := hex.DecodeString(cliId)
	if err != nil {
		return fmt.Errorf("invalid client id: %s", cliId)
	}
	cidstr := string(cid)
	if scInfo, ok := ClisInfo[cidstr]; !ok {
		return fmt.Errorf("client with id: %s not exists", cliId)
	} else {
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
					cname := AttrNameCombine(existAttr.name, attr.name)
					if aidx, ok := AttrIdxesMap[cname]; !ok {
						log.Error(
							"cname %s not found in AttrIdxesMap for subcli %s",
							cname, cliId)
					} else {
						aidx.InsertWaitUpdate(cidstr)
					}
				}
			}
		}
	}

	return nil
}

func processAttrDelete(data *etcd.Response) error {
	// if attrName is empty, we will delete all this client's subscription info.
	cliId, attrName := dmq.ExtractInfoFromDelKey(data.Node.Key)
	if cliId == "" {
		return fmt.Errorf("invalid attr delete notify key: %s", data.Node.Key)
	}

	cid, err := hex.DecodeString(cliId)
	if err != nil {
		return fmt.Errorf("invalid client id: %s", cliId)
	}
	cidstr := string(cid)
	if _, ok := ClisInfo[cidstr]; !ok {
		return fmt.Errorf("client not exists: %s", cliId)
	} else {
		// remove attrName from Attrs of this subclient, if attrName is empty,
		// remove all the attrs.
		if attrName == "" {
			for i := 0; i < len(ClisInfo[cidstr].Attrs); i++ {
				ClisInfo[cidstr].Attrs[i] = nil
			}
			ClisInfo[cidstr].Attrs = nil
		} else {
			attrNum := len(ClisInfo[cidstr].Attrs)
			for i := 0; i < attrNum; i++ {
				if ClisInfo[cidstr].Attrs[i].name == attrName {
					ClisInfo[cidstr].Attrs[attrNum-1], ClisInfo[cidstr].Attrs =
						nil,
						append(ClisInfo[cidstr].Attrs[:i],
							ClisInfo[cidstr].Attrs[i+1:]...)
					break
				}
			}
		}

		// remove all intervals containing attrName from segment tree
		for combineName, ival := range ClisInfo[cidstr].Intval {
			// Contains(s, substr string) always returns true if substr is ""
			if strings.Contains(combineName, attrName) {
				// remove inteval from segment tree index
				AttrIdxesMap[combineName].tree.Delete(ival)
				// delete key-value pair from ClisInfo's intval map
				delete(ClisInfo[cidstr].Intval, combineName)
			}
		}
		if len(ClisInfo[cidstr].Attrs) == 0 {
			// TODO: memory check http://stackoverflow.com/a/23231539/1115857
			ClisInfo[cidstr].Intval = nil
			ClisInfo[cidstr] = nil
			delete(ClisInfo, cidstr)
		}
	}

	return nil
}

func processAttrNotify(data *etcd.Response) error {
	log.Debug("recv notify: %s %v", data.Action, data.Node)

	if cmd, ok := AttrCmdTable[data.Action]; !ok {
		return fmt.Errorf("action %s not support", data.Action)
	} else {
		return cmd(data)
	}
}

func AttrWatcher(machines []string) {
	receiver := make(chan *etcd.Response)
	stop := make(chan bool)

	c, _ := GetEtcdClient(machines)

	prefix := dmq.GetSubAttrBase()
	recursive := true
	go c.Watch(prefix, 0, recursive, receiver, stop)

RecvLoop:
	for {
		select {
		case data := <-receiver:
			if data == nil {
				log.Warning("receive nil notification in attribute watcher")
				break RecvLoop
			}
			if err := processAttrNotify(data); err != nil {
				log.Error("process attr notify with error: %v", err)
			}
		}
	}

	log.Info("restart attribute watcher with %v", machines)
	go AttrWatcher(machines)
}

func extractVnodeKey(val string) string {
	regstr := fmt.Sprintf("^%s/([0-9a-f]+)$", dmq.GetDataVnodeKey())
	regex := regexp.MustCompile(regstr)

	match := regex.FindStringSubmatch(val)

	if len(match) != 2 {
		return ""
	} else {
		return match[1]
	}
}

func processDataNodeNotify(data *etcd.Response) error {
	log.Debug("recv datanode notify: %s %v", data.Action, data.Node)

	if cmd, ok := DataNodeCmdTable[data.Action]; !ok {
		return fmt.Errorf("action %s not support", data.Action)
	} else {
		return cmd(data)
	}
}

func processDataNodeCreate(data *etcd.Response) error {
	vidHexStr := extractVnodeKey(data.Node.Key)
	if vidHexStr == "" {
		return fmt.Errorf("invalid vnode id key: %s", data.Node.Key)
	}
	vid, err := hex.DecodeString(vidHexStr)
	if err != nil {
		return fmt.Errorf("invalid vnode id: %v", err)
	}
	if data.Node.Dir {
		return fmt.Errorf("%s should not be a directory", data.Node.Key)
	}

	pnid := data.Node.Value
	pnode, ok := PnodeMap[pnid]
	if !ok {
		ec, err := EtcdCliPool.GetEtcdClient()
		if err != nil {
			return err
		}
		defer EtcdCliPool.RecycleEtcdClient(ec.Id)
		c := ec.Cli

		if pubAddr, err := GetPnodeBindAddr(c, pnid); err != nil {
			return err
		} else {
			pnode = &Pnode{
				id:       pnid,
				bindAddr: pubAddr,
				vnum:     0,
			}
			PnodeMap[pnid] = pnode
		}
	}
	vn := &Vnode{
		id: []byte(vid),
		pn: pnode,
	}
	if Rtable.JoinVnode(vn, false) {
		pnode.vnum++
	}

	return nil
}

func processDataNodeUpdate(data *etcd.Response) error {
	return nil
}

func processDataNodeDelete(data *etcd.Response) error {
	vidHexStr := extractVnodeKey(data.Node.Key)
	if vidHexStr == "" {
		return fmt.Errorf("invalid vnode id key: %s", data.Node.Key)
	}
	vid, err := hex.DecodeString(vidHexStr)
	if err != nil {
		return fmt.Errorf("invalid vnode id: %v", err)
	}

	if pos, vn := Rtable.Search([]byte(vid)); vn != nil {
		vnum := len(Rtable.vns)
		// delete this vnode from rtable vnode list
		Rtable.vns[vnum-1], Rtable.vns =
			nil, append(Rtable.vns[:pos], Rtable.vns[pos+1:]...)

		vn.pn.vnum--
		if vn.pn.vnum == 0 {
			// remove pnode from PnodeMap
			delete(PnodeMap, vn.pn.id)
		}
	}

	return nil
}

func DataNodeWatcher(machines []string) {
	receiver := make(chan *etcd.Response)
	stop := make(chan bool)

	c, _ := GetEtcdClient(machines)

	prefix := dmq.GetDataVnodeKey()
	recursive := true
	go c.Watch(prefix, 0, recursive, receiver, stop)

	for {
		select {
		case data := <-receiver:
			if err := processDataNodeNotify(data); err != nil {
				log.Error("process chord notify with error: %v", err)
			}
		}
	}
}
