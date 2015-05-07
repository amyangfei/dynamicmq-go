package main

import (
	"errors"
)

var (
	ProcessLater = errors.New("process Later")
)

var (
	// default expire time for publisher, 3min
	PubDfltExpire int64 = 3 * 60

	// heartbeat interval to datanode, 5min
	HbIntervalToDN int = 5 * 60
)
