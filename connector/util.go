package main

import (
	"encoding/json"
	"fmt"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"math"
)

var FloatMinDiff = 0.00001

func AddReplyMultiBulk(target *[]byte, cnts []string) {
	addReply(target, fmt.Sprintf("*%d%s", len(cnts), dmq.Crlf))
	for _, cnt := range cnts {
		AddReplyBulk(target, cnt)
	}
}

func AddReplyBulk(target *[]byte, cnt string) {
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

func FloatCompare(a, b float64) int {
	var diff float64 = a - b
	if math.Abs(diff) < FloatMinDiff {
		return 0
	}
	if diff > 0 {
		return 1
	} else {
		return -1
	}
}

func AttrMarshal(attr *Attribute) ([]byte, error) {
	data := map[string]interface{}{
		"use": attr.use,
	}
	switch int(attr.use) {
	case AttrUseField["strval"]:
		data["strval"] = attr.strval
	case AttrUseField["range"]:
		data["low"] = attr.low
		data["high"] = attr.high
	case AttrUseField["extra"]:
		data["extra"] = attr.extra
	}
	return json.Marshal(data)
}
