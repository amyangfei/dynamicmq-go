package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"github.com/coreos/go-etcd/etcd"
	"sync"
)

var (
	NotifyCmdTable = map[string]func(data *etcd.Response) error{
		dmq.EtcdActionCreate: processAttrCreate,
		dmq.EtcdActionUpdate: processAttrUpdate,
		dmq.EtcdActionDelete: processAttrDelete,
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
	cliIdHexStr, attrName := dmq.ExtractInfoFromSubKey(data.Node.Key)
	if cliIdHexStr == "" || attrName == "" {
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
	if _, ok := ClisInfo[cidstr]; !ok {
		connId, err := GetSubCliConnId(cliIdHexStr, EtcdCliPool)
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
	}
	ClisInfo[cidstr].Attrs = append(ClisInfo[cidstr].Attrs, attr)
	ClisInfo[cidstr].AttrMap[attr.name] = attr

	return nil
}

func processAttrUpdate(data *etcd.Response) error {
	cliIdHexStr, attrName := dmq.ExtractInfoFromSubKey(data.Node.Key)
	if cliIdHexStr == "" || attrName == "" {
		return fmt.Errorf("invalid attr update notify key: %s", data.Node.Key)
	}
	attr, err := extractAttrFromSubVal(data.Node.Value)
	if err != nil {
		return err
	}
	attr.name = attrName

	// TODO: support more attribute expression besides 'range'
	if int(attr.use) != dmq.AttrUseField[dmq.AttrUseRange] {
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
		return fmt.Errorf("client with id: %s not exists", cliIdHexStr)
	} else {
		if oldAttr, ok := scInfo.AttrMap[attr.name]; !ok {
			return fmt.Errorf("attribute %s not in %s's SubCliInfo",
				attr.name, cliIdHexStr)
		} else {
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
		ClisInfo[cidstr].lock.Lock()
		// remove attrName from Attrs of this subclient, if attrName is empty,
		// remove all the attrs.
		if attrName == "" {
			for i := 0; i < len(ClisInfo[cidstr].Attrs); i++ {
				delete(ClisInfo[cidstr].AttrMap, ClisInfo[cidstr].Attrs[i].name)
				ClisInfo[cidstr].Attrs[i] = nil
			}
			ClisInfo[cidstr].AttrMap = nil
			ClisInfo[cidstr].Attrs = nil
		} else {
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

func processAttrNotify(data *etcd.Response) error {
	log.Debug("recv notify: %s %v", data.Action, data.Node)

	if cmd, ok := NotifyCmdTable[data.Action]; !ok {
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

	for {
		select {
		case data := <-receiver:
			if err := processAttrNotify(data); err != nil {
				log.Error("process attr notify with error: %v", err)
			}
		}
	}
}
