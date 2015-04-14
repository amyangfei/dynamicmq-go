package main

type SrvConfig struct {
	// server config
	NodeId         string
	MatchTCPBind   string
	ConnTCPBind    string
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
