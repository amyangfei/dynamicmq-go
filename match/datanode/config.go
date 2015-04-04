package main

type SrvConfig struct {
	// basic config
	BindIp   string
	LogLevel string
	LogFile  string
	PidFile  string

	// serf config
	SerfBinPath   string
	SerfNodeName  string
	SerfBindPort  int
	SerfBindAddr  string
	SerfRPCAddr   string
	SerfEvHandler string
	SerfLogFile   string
	SerfCfgFile   string

	// chord config
	Hostname      string
	BindPort      int
	BindAddr      string
	RPCAddr       string
	Workdir       string
	NumVnodes     int
	NumSuccessors int
	HashBits      int
	StartHash     []byte
	Entrypoint    string
}
