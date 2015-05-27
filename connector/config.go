package main

// SrvConfig is the main configuration for connector
type SrvConfig struct {
	// server config
	NodeID         string
	BindIP         string
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

	// redis
	MetaRedisAddr    string
	AttrRedisAddr    string
	RedisIDleTimeout int // in second
	RedisMaxIDle     int
	RedisMaxActive   int

	// etcd
	EtcdMachines     []string
	AttrEtcdMachines []string
	EtcdPoolSize     int
	EtcdPoolMaxSize  int
}
