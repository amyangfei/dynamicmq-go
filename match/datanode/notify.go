package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"github.com/garyburd/redigo/redis"
	"sync"
)

var (
	notifyCmdTable = map[string]func(msg redis.PMessage, attrRCPool *dmq.RedisCliPool) error{
		dmq.RedisNotifySet: processAttrCreateOrUpdate,
		dmq.RedisNotifyDel: processAttrDelete,
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

	// check whether is stored on this datanode
	cidHash := dmq.GenHash(cid, Config.HashFunc)
	candvn := ChordNode.Rtable.Search(cidHash)
	if candvn.Pnode.Hostname != ChordNode.Config.Hostname {
		// ignore
		return nil
	}

	cidstr := string(cid)
	if scInfo, ok := ClisInfo[cidstr]; !ok {
		// create new sub client
		connID, err := dmq.GetSubConnID(MetaRCPool, cliIDHexStr)
		if err != nil {
			return err
		}
		ClisInfo[cidstr] = &SubCliInfo{
			lock:    new(sync.RWMutex),
			Cid:     cid,
			CidHash: cidHash,
			ConnID:  connID,
			Attrs:   make([]*Attribute, 0),
			AttrMap: make(map[string]*Attribute),
		}
		ClisInfo[cidstr].Attrs = append(ClisInfo[cidstr].Attrs, attr)
		ClisInfo[cidstr].AttrMap[attr.name] = attr
	} else {
		if oldAttr, ok := scInfo.AttrMap[attr.name]; !ok {
			// create new attribute
			ClisInfo[cidstr].Attrs = append(ClisInfo[cidstr].Attrs, attr)
			ClisInfo[cidstr].AttrMap[attr.name] = attr
		} else {
			// update attribute
			log.Debug("update %s's attr %s", cliIDHexStr, attr.name)
			scInfo.lock.Lock()
			defer scInfo.lock.Unlock()
			if oldAttr.low != attr.low {
				oldAttr.low = attr.low
			}
			if oldAttr.high != attr.high {
				oldAttr.high = attr.high
			}
		}
	}

	return nil
}

func processAttrDelete(data redis.PMessage, attrRCPool *dmq.RedisCliPool) error {
	subkey := dmq.IdxSepString(data.Channel, ":", -1)
	cliID, attrName := dmq.ExtractInfoFromDelKey(subkey)
	if cliID == "" {
		return fmt.Errorf("invalid attr delete notify key: %s", subkey)
	}

	cid, err := hex.DecodeString(cliID)
	if err != nil {
		return fmt.Errorf("invalid client id: %s", cliID)
	}
	cidstr := string(cid)

	scInfo, ok := ClisInfo[cidstr]
	if !ok {
		return fmt.Errorf("client not exists: %s", cliID)
	}
	scInfo.lock.Lock()
	attrNum := len(scInfo.Attrs)
	for i := 0; i < attrNum; i++ {
		if scInfo.Attrs[i].name == attrName {
			delete(scInfo.AttrMap, attrName)
			scInfo.Attrs[attrNum-1], scInfo.Attrs =
				nil, append(scInfo.Attrs[:i], scInfo.Attrs[i+1:]...)
			break
		}
	}
	scInfo.lock.Unlock()

	if len(scInfo.Attrs) == 0 {
		// TODO: memory check http://stackoverflow.com/a/23231539/1115857
		// FIXME: ClisInfo[cidstr] = nil
		delete(ClisInfo, cidstr)
	}

	return nil
}

func processAttrNotify(msg redis.PMessage, attrRCPool *dmq.RedisCliPool) error {
	cmd, ok := notifyCmdTable[string(msg.Data)]
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
	psc.PSubscribe("__keyspace*__:/sub/attr/*")

RecvLoop:
	for {
		switch v := psc.Receive().(type) {
		case redis.Message:
			log.Debug("redis notify message channel: %s data: %s", v.Channel, v.Data)
		case redis.PMessage:
			log.Debug("redis notify pmessage channel: %s data: %s", v.Channel, v.Data)
			receiver <- v
		case redis.Subscription:
			log.Debug("redis notify channel: %s kind: %s", v.Channel, v.Kind)
		case error:
			log.Error("redis notify error: %v", v)
			stop <- true
			break RecvLoop
		}
	}

	log.Info("restart attribute watcher...")
	go attrWatcher(attrRCPool)
}
