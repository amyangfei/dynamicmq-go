package main

// RouterManager status
const (
	RmStatusInit = iota
	RmStatusOk
	RmStatusError
)

var (
	MatchNodeDfltExpire = 15 * 60
)
