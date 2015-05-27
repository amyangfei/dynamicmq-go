package main

// SrvConfig is the main config struct for authsrv
type SrvConfig struct {
	// server config
	NodeID         string
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
