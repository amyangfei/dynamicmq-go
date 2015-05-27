package main

// RouterManager status
const (
	RmStatusInit = iota
	RmStatusOk
	RmStatusError
)

var (
	matchNodeDfltExpire = 15 * 60

	// heartbeat interval to other dispatcher, 5min
	hbIntervalToDisp = 5 * 60
)
