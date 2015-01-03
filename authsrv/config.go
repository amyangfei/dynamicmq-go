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

	// TODO: persistent storage config
}
