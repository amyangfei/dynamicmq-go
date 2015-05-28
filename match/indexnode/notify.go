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
	attrCmdTable = map[string]func(msg redis.PMessage, attrRCPool *dmq.RedisCliPool) error{
		dmq.RedisNotifySet: processAttrCreateOrUpdate,
		dmq.RedisNotifyDel: processAttrDelete,
	}

	dataNodeCmdTable = map[string]func(data *etcd.Response) error{
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
		strval, ok := attrData["strval"].(string)
		if !ok {
			return nil, fmt.Errorf("invalid sub record, strval not found")
		}
		attr.strval = strval
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
		extra, ok := attrData["extra"].(string)
		if !ok {
			return nil, fmt.Errorf("invalid sub record, extra not found")
		}
		attr.extra = extra
	default:
		return nil, fmt.Errorf("invalid use field: %s", use)
	}

	return attr, nil
}

func processAttrCreateOrUpdate(data redis.PMessage, attrRCPool *dmq.RedisCliPool) error {
	subkey := dmq.IdxSepString(data.Channel, ":", -1)
	cliIDHexStr, attrName := dmq.ExtractInfoFromSubKey(subkey)
	if cliIDHexStr == "" || attrName == "" {
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

	cid, err := hex.DecodeString(cliIDHexStr)
	if err != nil {
		return fmt.Errorf("invalid client id: %s", cliIDHexStr)
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
		for _, subattr := range scInfo.Attrs {
			if subattr.name == attr.name {
				isNewAttr = false
				break
			}
		}
		if isNewAttr {
			// Iterate each attribute and make up a new attribute pair with the
			// new attrbute and insert it into corresponding AttrIndex.
			for _, subattr := range scInfo.Attrs {
				xattr, yattr := attrSort(subattr, attr)
				cname := attrNameCombine(xattr.name, yattr.name)
				aidx, ok := AttrIdxesMap[cname]
				if !ok {
					return fmt.Errorf("attribute combine name %s not found", cname)
				}
				xmin, xmax, ymin, ymax := attrRangeFilter(xattr, yattr)
				ival := aidx.insertCliAttr(
					xmin, xmax, ymin, ymax, &scInfo.Cid)
				scInfo.Intval[cname] = ival

				log.Debug("attr combine %s-%s", xattr.name, yattr.name)
			}
			scInfo.Attrs = append(scInfo.Attrs, attr)
		} else {
			scInfo.attrUpdate(attr)
		}
	}

	return nil
}

func processAttrDelete(data redis.PMessage, attrRCPool *dmq.RedisCliPool) error {
	subkey := dmq.IdxSepString(data.Channel, ":", -1)
	cliIDHexStr, attrName := dmq.ExtractInfoFromSubKey(subkey)
	if cliIDHexStr == "" || attrName == "" {
		return fmt.Errorf("invalid attr create or update notify key: %s", subkey)
	}

	cid, err := hex.DecodeString(cliIDHexStr)
	if err != nil {
		return fmt.Errorf("invalid client id: %s", cliIDHexStr)
	}
	cidstr := string(cid)
	scInfo, ok := ClisInfo[cidstr]
	if !ok {
		return fmt.Errorf("client not exists: %s", cliIDHexStr)
	}
	// remove attrName from Attrs of this subclient, if attrName is empty,
	// remove all the attrs.
	if attrName == "" {
		for i := 0; i < len(scInfo.Attrs); i++ {
			scInfo.Attrs[i] = nil
		}
		scInfo.Attrs = nil
	} else {
		attrNum := len(scInfo.Attrs)
		for i := 0; i < attrNum; i++ {
			if scInfo.Attrs[i].name == attrName {
				scInfo.Attrs[attrNum-1], scInfo.Attrs =
					nil,
					append(scInfo.Attrs[:i],
						scInfo.Attrs[i+1:]...)
				break
			}
		}
	}

	// remove all intervals containing attrName from segment tree
	for combineName, ival := range scInfo.Intval {
		// Contains(s, substr string) always returns true if substr is ""
		if strings.Contains(combineName, attrName) {
			// remove inteval from segment tree index
			AttrIdxesMap[combineName].tree.Delete(ival)
			// delete key-value pair from ClisInfo's intval map
			delete(scInfo.Intval, combineName)
		}
	}
	if len(scInfo.Attrs) == 0 {
		// TODO: memory check http://stackoverflow.com/a/23231539/1115857
		scInfo.Intval = nil
		scInfo = nil
		delete(ClisInfo, cidstr)
	}

	return nil
}

func processAttrNotify(msg redis.PMessage, attrRCPool *dmq.RedisCliPool) error {
	cmd, ok := attrCmdTable[string(msg.Data)]
	if !ok {
		return fmt.Errorf("cmd %s not support", msg.Data)
	}
	return cmd(msg, attrRCPool)
}

func attrProcesser(receiver chan redis.PMessage, stop chan bool, attrRCPool *dmq.RedisCliPool) {
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

func attrWatcher(attrRCPool *dmq.RedisCliPool) {
	receiver := make(chan redis.PMessage)
	stop := make(chan bool)

	go attrProcesser(receiver, stop, attrRCPool)

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
	go attrWatcher(attrRCPool)
}

func extractVnodeKey(val string) string {
	regstr := fmt.Sprintf("^%s/([0-9a-f]+)$", dmq.GetDataVnodeKey())
	regex := regexp.MustCompile(regstr)

	match := regex.FindStringSubmatch(val)

	if len(match) != 2 {
		return ""
	}
	return match[1]
}

func processDataNodeNotify(data *etcd.Response) error {
	log.Debug("recv datanode notify: %s %v", data.Action, data.Node)

	cmd, ok := dataNodeCmdTable[data.Action]
	if !ok {
		return fmt.Errorf("action %s not support", data.Action)
	}
	return cmd(data)
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

		pubAddr, err := getPnodeBindAddr(c, pnid)
		if err != nil {
			return err
		}
		pnode = &Pnode{
			id:       pnid,
			bindAddr: pubAddr,
			vnum:     0,
		}
		PnodeMap[pnid] = pnode
	}
	vn := &Vnode{
		id: []byte(vid),
		pn: pnode,
	}
	if Rtable.joinVnode(vn, false) {
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

func dataNodeWatcher(machines []string) {
	receiver := make(chan *etcd.Response)
	stop := make(chan bool)

	c, _ := getEtcdClient(machines)

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
