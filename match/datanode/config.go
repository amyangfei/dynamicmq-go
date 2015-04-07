package main

type SrvConfig struct {
	// basic config
	BindIp         string
	LogLevel       string
	LogFile        string
	PidFile        string
	TCPRecvBufSize int
	TCPSendBufSize int
	TCPBufInsNum   int
	TCPBufioNum    int

	// serf config
	SerfBinPath   string
	SerfNodeName  string
	SerfBindPort  int
	SerfBindAddr  string
	SerfRPCPort   int
	SerfRPCAddr   string
	SerfEvHandler string
	SerfLogFile   string
	SerfCfgFile   string

	// chord config
	Hostname      string
	BindPort      int
	BindAddr      string
	RPCPort       int
	RPCAddr       string
	Workdir       string
	NumVnodes     int
	NumSuccessors int
	HashBits      int
	StartHash     []byte
	Entrypoint    string
}
