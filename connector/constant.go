package main

import (
	"errors"
)

var (
	errProcessLater = errors.New("process Later")
)

// message type
const (
	PushMsg = iota + 1
)
