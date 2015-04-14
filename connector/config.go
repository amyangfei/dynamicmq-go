package main

type SrvConfig struct {
	// server config
	NodeId         string
	BindIp         string
	SubTCPBind     string
	RouterTCPBind  string
	AuthSrvAddr    string
	MaxProc        int
	WorkingDir     string
	LogLevel       string
	LogFile        string
	PidFile        string
	TCPRecvBufSize int
	TCPSendBufSize int
	TCPBufInsNum   int
	TCPBufioNum    int
	SubKeepalive   int
	DispKeepalive  int
	Capacity       int

	// redis for message cache
	RedisEndPoint    string
	RedisIdleTimeout int // in second
	RedisMaxIdle     int
	RedisMaxActive   int

	// etcd
	EtcdMachines    []string
	EtcdPoolSize    int
	EtcdPoolMaxSize int
}
