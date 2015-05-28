package main

import (
	"errors"
)

var (
	errProcessLater = errors.New("process Later")
)

var (
	// default expire time for publisher, 3min
	pubDfltExpire int64 = 3 * 60

	// heartbeat interval to datanode, 5min
	hbIntervalToDN = 5 * 60
)
