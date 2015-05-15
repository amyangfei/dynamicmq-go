package main

import (
	"hash"
)

type SrvConfig struct {
	// basic config
	NodeId         string
	BindIp         string
	PubTCPBind     string
	Workdir        string
	LogLevel       string
	LogFile        string
	PidFile        string
	TCPRecvBufSize int
	TCPSendBufSize int
	TCPBufInsNum   int
	TCPBufioNum    int
	HashFunc       func() hash.Hash
	UpdateCacheThr int
	FlushInterval  int
	FlushTimeout   int

	// etcd
	EtcdMachines    []string
	EtcdPoolSize    int
	EtcdPoolMaxSize int

	// redis
	AttrRedisAddrs   []string
	RedisIdleTimeout int // in second
	RedisMaxIdle     int
	RedisMaxActive   int
}
