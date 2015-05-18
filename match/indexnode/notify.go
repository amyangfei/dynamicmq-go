package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"github.com/coreos/go-etcd/etcd"
	"github.com/garyburd/redigo/redis"
	"regexp"
	"strings"
)

var (
	AttrCmdTable = map[string]func(msg redis.PMessage, attrRCPool *dmq.RedisCliPool) error{
		dmq.RedisNotifySet: processAttrCreateOrUpdate,
		dmq.RedisNotifyDel: processAttrDelete,
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

func processAttrCreateOrUpdate(data redis.PMessage, attrRCPool *dmq.RedisCliPool) error {
	subkey := dmq.IdxSepString(data.Channel, ":", -1)
	cliIdHexStr, attrName := dmq.ExtractInfoFromSubKey(subkey)
	if cliIdHexStr == "" || attrName == "" {
		return fmt.Errorf("invalid attr create or update notify key: %s", subkey)
	}

	attrVal, err := dmq.RedisKVGet(attrRCPool, subkey)
	if err != nil {
		return err
	}
	attr, err := extractAttrFromSubVal(attrVal)
	if err != nil {
		return err
	}
	attr.name = attrName

	// TODO: support more attribute expression besides 'range'
	if int(attr.use) != dmq.AttrUseField["range"] {
		return nil
	}

	cid, err := hex.DecodeString(cliIdHexStr)
	if err != nil {
		return fmt.Errorf("invalid client id: %s", cliIdHexStr)
	}
	cidstr := string(cid)

	if scInfo, ok := ClisInfo[cidstr]; !ok {
		ClisInfo[cidstr] = &SubCliInfo{
			Cid:     cid,
			CidHash: dmq.GenHash(cid, Config.HashFunc),
			Attrs:   make([]*Attribute, 0),
			Intval:  make(map[string]*Interval),
		}
		ClisInfo[cidstr].Attrs = append(ClisInfo[cidstr].Attrs, attr)
	} else {
		isNewAttr := true
		for _, subattr := range ClisInfo[cidstr].Attrs {
			if subattr.name == attr.name {
				isNewAttr = false
				break
			}
		}
		if isNewAttr {
			// Iterate each attribute and make up a new attribute pair with the
			// new attrbute and insert it into corresponding AttrIndex.
			for _, subattr := range scInfo.Attrs {
				xattr, yattr := AttrSort(subattr, attr)
				cname := AttrNameCombine(xattr.name, yattr.name)
				if aidx, ok := AttrIdxesMap[cname]; !ok {
					return fmt.Errorf("attribute combine name %s not found", cname)
				} else {
					xmin, xmax, ymin, ymax := attrRangeFilter(xattr, yattr)
					ival := aidx.InsertCliAttr(
						xmin, xmax, ymin, ymax, &scInfo.Cid)
					scInfo.Intval[cname] = ival
				}

				log.Debug("attr combine %s-%s", xattr.name, yattr.name)
			}
			scInfo.Attrs = append(scInfo.Attrs, attr)
		} else {
			scInfo.AttrUpdate(attr)
		}
	}

	return nil
}

func processAttrDelete(data redis.PMessage, attrRCPool *dmq.RedisCliPool) error {
	subkey := dmq.IdxSepString(data.Channel, ":", -1)
	cliIdHexStr, attrName := dmq.ExtractInfoFromSubKey(subkey)
	if cliIdHexStr == "" || attrName == "" {
		return fmt.Errorf("invalid attr create or update notify key: %s", subkey)
	}

	cid, err := hex.DecodeString(cliIdHexStr)
	if err != nil {
		return fmt.Errorf("invalid client id: %s", cliIdHexStr)
	}
	cidstr := string(cid)
	if _, ok := ClisInfo[cidstr]; !ok {
		return fmt.Errorf("client not exists: %s", cliIdHexStr)
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

func processAttrNotify(msg redis.PMessage, attrRCPool *dmq.RedisCliPool) error {
	if cmd, ok := AttrCmdTable[string(msg.Data)]; !ok {
		return fmt.Errorf("cmd %s not support", msg.Data)
	} else {
		return cmd(msg, attrRCPool)
	}
}

func AttrProcesser(receiver chan redis.PMessage, stop chan bool, attrRCPool *dmq.RedisCliPool) {
	for {
		select {
		case msg := <-receiver:
			if err := processAttrNotify(msg, attrRCPool); err != nil {
				log.Error("process attr notify with error: %v", err)
			}
		case <-stop:
			return
		}
	}
}

func AttrWatcher(attrRCPool *dmq.RedisCliPool) {
	receiver := make(chan redis.PMessage)
	stop := make(chan bool)

	go AttrProcesser(receiver, stop, attrRCPool)

	c := attrRCPool.GetConn()
	if c == nil {
		log.Error("failed to get redis connection")
		return
	}
	defer c.Close()
	psc := redis.PubSubConn{Conn: c}
	psc.PSubscribe(dmq.RedisSubAttrPattern)

RecvLoop:
	for {
		switch v := psc.Receive().(type) {
		case redis.PMessage:
			log.Debug("redis notify pmessage channel: %s data: %s", v.Channel, v.Data)
			receiver <- v
		case redis.Subscription:
			log.Debug("redis notify channel: %s kind: %s", v.Channel, v.Kind)
		case redis.Message:
			log.Debug("redis notify message channel: %s data: %s", v.Channel, v.Data)
		case error:
			log.Error("redis notify error: %v", v)
			stop <- true
			break RecvLoop
		}
	}

	log.Info("restart attribute watcher...")
	go AttrWatcher(attrRCPool)
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
