package main

type SrvConfig struct {
	// server config
	NodeId         string
	SubAuthTCPBind string
	PubAuthTCPBind string
	WorkingDir     string
	LogLevel       string
	LogFile        string
	PidFile        string
	SignKey        string

	// etcd config
	EtcdMachines    []string
	EtcdPoolSize    int
	EtcdPoolMaxSize int
}
