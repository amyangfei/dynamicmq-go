package main

import (
	"errors"
)

var (
	ProcessLater = errors.New("process Later")
)

// message type
const (
	PushMsg = iota + 1
)
