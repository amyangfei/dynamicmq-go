package main

import (
	"fmt"
)

var (
	crlf string = "\r\n"
)

func AddReplyMultiBulk(target *[]byte, cnts []string) {
	addReply(target, fmt.Sprintf("*%d%s", len(cnts), crlf))
	for _, cnt := range cnts {
		AddReplyBulk(target, cnt)
	}
}

func AddReplyBulk(target *[]byte, cnt string) {
	addReplyBulklen(target, cnt)
	addReply(target, cnt)
	addReply(target, crlf)
}

func addReplyBulklen(target *[]byte, cnt string) {
	slen := len(cnt)
	*target = append(*target, []byte(fmt.Sprintf("$%d\r\n", slen))...)
}

func addReply(target *[]byte, cnt string) {
	*target = append(*target, []byte(cnt)...)
}
