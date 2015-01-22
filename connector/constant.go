package main

import (
	"errors"
)

var (
	ProcessLater = errors.New("process Later")
)

var (
	AttrUseField = map[string]int{
		"strval": 1,
		"range":  2,
		"extra":  3,
	}
)

// message type
const (
	PushMsg = iota + 1
)
