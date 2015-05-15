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
	NotifyCmdTable = map[string]func(msg redis.PMessage, attrRCPool *dmq.RedisCliPool) error{
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
		connId, err := dmq.GetSubConnId(MetaRCPool, cliIdHexStr)
		if err != nil {
			return err
		}
		ClisInfo[cidstr] = &SubCliInfo{
			lock:    new(sync.RWMutex),
			Cid:     cid,
			CidHash: cidHash,
			ConnId:  connId,
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
			log.Debug("update %s's attr %s", cliIdHexStr, attr.name)
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
	cliId, attrName := dmq.ExtractInfoFromDelKey(subkey)
	if cliId == "" {
		return fmt.Errorf("invalid attr delete notify key: %s", subkey)
	}

	cid, err := hex.DecodeString(cliId)
	if err != nil {
		return fmt.Errorf("invalid client id: %s", cliId)
	}
	cidstr := string(cid)
	if _, ok := ClisInfo[cidstr]; !ok {
		return fmt.Errorf("client not exists: %s", cliId)
	} else {
		ClisInfo[cidstr].lock.Lock()
		attrNum := len(ClisInfo[cidstr].Attrs)
		for i := 0; i < attrNum; i++ {
			if ClisInfo[cidstr].Attrs[i].name == attrName {
				delete(ClisInfo[cidstr].AttrMap, attrName)
				ClisInfo[cidstr].Attrs[attrNum-1], ClisInfo[cidstr].Attrs =
					nil,
					append(ClisInfo[cidstr].Attrs[:i],
						ClisInfo[cidstr].Attrs[i+1:]...)
				break
			}
		}

		ClisInfo[cidstr].lock.Unlock()

		if len(ClisInfo[cidstr].Attrs) == 0 {
			// TODO: memory check http://stackoverflow.com/a/23231539/1115857
			// FIXME: ClisInfo[cidstr] = nil
			delete(ClisInfo, cidstr)
		}
	}

	return nil
}

func processAttrNotify(msg redis.PMessage, attrRCPool *dmq.RedisCliPool) error {
	if cmd, ok := NotifyCmdTable[string(msg.Data)]; !ok {
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
	psc := redis.PubSubConn{c}
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
	go AttrWatcher(attrRCPool)
}
