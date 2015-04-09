package main

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

	// etcd
	EtcdMachines []string
}
