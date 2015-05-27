package main

import (
	"encoding/json"
	"fmt"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"math"
)

var floatMinDiff = 0.00001

// MsgPack converts a dict to binary message in a resp like format
func MsgPack(msg map[string]interface{}) ([]byte, error) {
	bmsg, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}
	dlen := len(bmsg) + 2
	header := fmt.Sprintf("#%d%s", dlen, dmq.Crlf)
	bmsg = append([]byte(header), bmsg...)
	bmsg = append(bmsg, []byte(dmq.Crlf)...)
	return bmsg, nil
}

func addReplyMultiBulk(target *[]byte, cnts []string) {
	addReply(target, fmt.Sprintf("*%d%s", len(cnts), dmq.Crlf))
	for _, cnt := range cnts {
		addReplyBulk(target, cnt)
	}
}

func addReplyBulk(target *[]byte, cnt string) {
	addReplyBulklen(target, cnt)
	addReply(target, cnt)
	addReply(target, dmq.Crlf)
}

func addReplyBulklen(target *[]byte, cnt string) {
	slen := len(cnt)
	*target = append(*target, []byte(fmt.Sprintf("$%d\r\n", slen))...)
}

func addReply(target *[]byte, cnt string) {
	*target = append(*target, []byte(cnt)...)
}

func floatCompare(a, b float64) int {
	diff := a - b
	if math.Abs(diff) < floatMinDiff {
		return 0
	}
	if diff > 0 {
		return 1
	}
	return -1
}

// AttrMarshal converts an attribute to json format byte array
func AttrMarshal(attr *Attribute) ([]byte, error) {
	data := map[string]interface{}{
		"use": attr.use,
	}
	switch int(attr.use) {
	case dmq.AttrUseField["strval"]:
		data["strval"] = attr.strval
	case dmq.AttrUseField["range"]:
		data["low"] = attr.low
		data["high"] = attr.high
	case dmq.AttrUseField["extra"]:
		data["extra"] = attr.extra
	}
	return json.Marshal(data)
}
