package main

type SrvConfig struct {
	// server config
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

	// zookeeper
	ZookeeperAddr string
}
