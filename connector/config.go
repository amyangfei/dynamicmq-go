package main

type SrvConfig struct {
	// server config
	NodeId         string
	SubTCPBind     string
	RouterTCPBind  string
	MaxProc        int
	WorkingDir     string
	LogLevel       string
	LogFile        string
	PidFile        string
	TCPRecvBufSize int
	TCPSendBufSize int
	TCPBufInsNum   int
	TCPBufioNum    int

	// redis for message cache
	RedisEndPoint    string
	RedisIdleTimeout int // in second
	RedisMaxIdle     int
	RedisMaxActive   int

	// zookeeper
	ZookeeperAddr string
}
