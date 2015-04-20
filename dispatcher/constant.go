package main

// RouterManager status
const (
	RmStatusInit = iota
	RmStatusOk
	RmStatusError
)

var (
	MatchNodeDfltExpire = 15 * 60

	// heartbeat interval to other dispatcher, 5min
	HbIntervalToDisp int = 5 * 60
)
