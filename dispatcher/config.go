package main

// SrvConfig represents server basic configuration
type SrvConfig struct {
	// server config
	NodeID         string
	BindIP         string
	MatchTCPPort   int
	MatchTCPBind   string
	MaxProc        int
	WorkingDir     string
	LogLevel       string
	LogFile        string
	PidFile        string
	TCPRecvBufSize int
	TCPSendBufSize int
	TCPBufInsNum   int
	TCPBufioNum    int
	HeartbeatIval  int

	// etcd
	EtcdMachines    []string
	EtcdPoolSize    int
	EtcdPoolMaxSize int
}
