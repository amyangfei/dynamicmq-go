package main

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"github.com/coreos/go-etcd/etcd"
	"regexp"
)

var (
	CmdTable = map[string]func(data *etcd.Response) error{
		dmq.EtcdActionCreate: processAttrCreate,
		dmq.EtcdActionUpdate: processAttrUpdate,
		dmq.EtcdActionDelete: processAttrDelete,
	}
)

func extractInfoFromSubKey(subkey string) (string, string) {
	regStr := fmt.Sprintf("^%s$", dmq.GetSubAttrKey("([0-9a-f]{24})", "([\\S]+)"))
	regex := regexp.MustCompile(regStr)

	match := regex.FindStringSubmatch(subkey)
	if len(match) != 3 {
		return "", ""
	} else {
		return match[1], match[2]
	}
}

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
	cliId, attrName := extractInfoFromSubKey(data.Node.Key)
	if cliId == "" || attrName == "" {
		return fmt.Errorf("invalid attr craete notify key: %s", data.Node.Key)
	}
	attr, err := extractAttrFromSubVal(data.Node.Value)
	if err != nil {
		return err
	}
	attr.name = attrName

	// TODO: support more attribute expression beyond 'range'
	if int(attr.use) != dmq.AttrUseField["range"] {
		return nil
	}

	bid, err := hex.DecodeString(cliId)
	if err != nil {
		return fmt.Errorf("invalid client id: %s", cliId)
	}
	cidstr := string(bid)
	if _, ok := CliAttrs[cidstr]; !ok {
		CliAttrs[cidstr] = make([]*Attribute, 0)
		CliAttrs[cidstr] = append(CliAttrs[cidstr], attr)
	} else {
		for _, subattr := range CliAttrs[cidstr] {
			// TODO: insert subclient to index tree
			log.Debug("attr combine %s-%s", subattr.name, attr.name)
		}
		CliAttrs[cidstr] = append(CliAttrs[cidstr], attr)
	}

	return nil
}

func processAttrUpdate(data *etcd.Response) error {
	return nil
}

func processAttrDelete(data *etcd.Response) error {
	return nil
}

func processAttrNotify(data *etcd.Response) error {
	log.Debug("recv notify: %s %v", data.Action, data.Node)

	if cmd, ok := CmdTable[data.Action]; !ok {
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
			processAttrNotify(data)
		}
	}
}
