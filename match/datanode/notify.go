package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"github.com/coreos/go-etcd/etcd"
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
	cidstr := string(cid)
	if _, ok := ClisInfo[cidstr]; !ok {
		ClisInfo[cidstr] = &SubCliInfo{
			Cid:     cid,
			CidHash: dmq.GenHash(cid, Config.HashFunc),
			Attrs:   make([]*Attribute, 0),
		}
	}
	ClisInfo[cidstr].Attrs = append(ClisInfo[cidstr].Attrs, attr)

	return nil
}

func processAttrUpdate(data *etcd.Response) error {
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

		if len(ClisInfo[cidstr].Attrs) == 0 {
			// TODO: memory check http://stackoverflow.com/a/23231539/1115857
			ClisInfo[cidstr] = nil
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
