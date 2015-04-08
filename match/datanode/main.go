package main

import (
	"crypto/sha1"
	"encoding/hex"
	"flag"
	"fmt"
	"github.com/amyangfei/dynamicmq-go/chord"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"github.com/op/go-logging"
	"github.com/rakyll/globalconf"
	"os"
	"runtime"
	"strconv"
	"time"
)

var Config *SrvConfig
var log = logging.MustGetLogger("dynamicmq-match-datanode")
var serfLog *os.File

func InitConfig(configFile, entrypoint, starthash string) error {
	conf, err := globalconf.NewWithOptions(&globalconf.Options{
		Filename: configFile,
	})
	if err != nil {
		return err
	}

	basicFlagSet := flag.NewFlagSet("basic", flag.PanicOnError)
	basicFlagSet.String("bind_ip", "127.0.0.1", "server bind ip")
	basicFlagSet.String("workdir", ".", "server working dir")
	basicFlagSet.String("log_level", "DEBUG", "log level")
	basicFlagSet.String("log_file", "./datanode.log", "log file path")
	basicFlagSet.String("pid_file", "./datanode_pid", "pid file")
	basicFlagSet.Int("tcp_recvbuf_size", 2048, "tcp receive buffer size")
	basicFlagSet.Int("tcp_sendbuf_size", 2048, "tcp send buffer size")
	basicFlagSet.Int("tcp_bufio_num", 64, "bufio num for each cache instance")

	serfFlagSet := flag.NewFlagSet("basic", flag.PanicOnError)
	serfFlagSet.String("bin_path", "/usr/local/bin/path", "serf bin path")
	serfFlagSet.String("node_name", "serf0101", "serf node name")
	serfFlagSet.Int("bind_port", 7946, "serf bind port")
	serfFlagSet.String("rpc_addr", "127.0.0.1:7373", "serf rpc addr")
	serfFlagSet.String("ev_handler", "./serfev_handler.py", "serf event handler")
	serfFlagSet.String("log_file", "./serf.log", "serf log file")

	chordFlagSet := flag.NewFlagSet("chord", flag.PanicOnError)
	chordFlagSet.String("hostname", "chod0101", "chord hostname")
	chordFlagSet.Int("bind_port", 5000, "chord bind port")
	chordFlagSet.Int("rpc_port", 5500, "chord rpc port")
	chordFlagSet.Int("num_vnodes", 16, "chord virtual node numbers")
	chordFlagSet.Int("num_successors", 3, "chord successor node numbers")
	chordFlagSet.Int("hash_bits", 160, "chord hash bits")

	globalconf.Register("basic", basicFlagSet)
	globalconf.Register("serf", serfFlagSet)
	globalconf.Register("chord", chordFlagSet)

	conf.ParseAll()

	Config = &SrvConfig{}

	Config.BindIp = basicFlagSet.Lookup("bind_ip").Value.String()
	Config.Workdir = basicFlagSet.Lookup("workdir").Value.String()
	Config.LogLevel = basicFlagSet.Lookup("log_level").Value.String()
	Config.LogFile = basicFlagSet.Lookup("log_file").Value.String()
	Config.PidFile = basicFlagSet.Lookup("pid_file").Value.String()
	Config.TCPRecvBufSize, err =
		strconv.Atoi(basicFlagSet.Lookup("tcp_recvbuf_size").Value.String())
	Config.TCPSendBufSize, err =
		strconv.Atoi(basicFlagSet.Lookup("tcp_sendbuf_size").Value.String())
	Config.TCPBufioNum, err =
		strconv.Atoi(basicFlagSet.Lookup("tcp_bufio_num").Value.String())
	Config.TCPBufInsNum = runtime.NumCPU()

	Config.SerfBinPath = serfFlagSet.Lookup("bin_path").Value.String()
	Config.SerfNodeName = serfFlagSet.Lookup("node_name").Value.String()
	Config.SerfBindPort, err =
		strconv.Atoi(serfFlagSet.Lookup("bind_port").Value.String())
	Config.SerfBindAddr = fmt.Sprintf("0.0.0.0:%d", Config.SerfBindPort)
	Config.SerfRPCAddr = serfFlagSet.Lookup("rpc_addr").Value.String()
	Config.SerfEvHandler = serfFlagSet.Lookup("ev_handler").Value.String()
	Config.SerfLogFile = serfFlagSet.Lookup("log_file").Value.String()

	Config.Hostname = chordFlagSet.Lookup("hostname").Value.String()
	Config.BindPort, err =
		strconv.Atoi(chordFlagSet.Lookup("bind_port").Value.String())
	Config.BindAddr = fmt.Sprintf("%s:%d", Config.BindIp, Config.BindPort)
	Config.RPCPort, err =
		strconv.Atoi(chordFlagSet.Lookup("rpc_port").Value.String())
	Config.RPCAddr = fmt.Sprintf("%s:%d", Config.BindIp, Config.RPCPort)
	Config.NumVnodes, err =
		strconv.Atoi(chordFlagSet.Lookup("num_vnodes").Value.String())
	Config.NumSuccessors, err =
		strconv.Atoi(chordFlagSet.Lookup("num_successors").Value.String())
	Config.HashBits, err =
		strconv.Atoi(chordFlagSet.Lookup("hash_bits").Value.String())

	Config.Entrypoint = entrypoint
	if len(starthash)%2 == 1 {
		starthash = "0" + starthash
	}
	if sh, err := hex.DecodeString(starthash); err != nil {
		return err
	} else if len(sh) != Config.HashBits/8 {
		return fmt.Errorf("error starthash hex string length %d, should be %d",
			len(starthash), Config.HashBits/8*2)
	} else {
		Config.StartHash = sh[:]
	}

	return nil
}

func InitLog(logFile, serfLogFile string) error {
	var format = logging.MustStringFormatter(
		"%{time:2006-01-02 15:04:05.000} [%{level:.4s}] %{id:03x} [%{shortfunc}] %{message}",
	)

	f, err := os.OpenFile(logFile, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	backend1 := logging.NewLogBackend(f, "", 0)
	backend1Formatter := logging.NewBackendFormatter(backend1, format)
	logging.SetBackend(backend1Formatter)
	logging.SetLevel(logging.DEBUG, "dynamicmq-match-datanode")

	serfLog, err = os.OpenFile(serfLogFile, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	return nil
}

func InitServer() error {
	log.Info("Datanode server is starting...")
	return nil
}

func chordRoutine() {
	conf := &chord.NodeConfig{
		Serf: &chord.SerfConfig{
			BinPath:   Config.SerfBinPath,
			NodeName:  Config.SerfNodeName,
			BindAddr:  Config.SerfBindAddr,
			RPCAddr:   Config.SerfRPCAddr,
			EvHandler: Config.SerfEvHandler,
		},
		Hostname:       Config.Hostname,
		HostIp:         Config.BindIp,
		BindAddr:       Config.BindAddr,
		RPCAddr:        Config.RPCAddr,
		NumVnodes:      Config.NumVnodes,
		NumSuccessors:  Config.NumSuccessors,
		HashFunc:       sha1.New,
		HashBits:       Config.HashBits,
		StartHash:      Config.StartHash,
		Entrypoint:     Config.Entrypoint,
		TCPRecvBufSize: Config.TCPRecvBufSize,
		TCPSendBufSize: Config.TCPSendBufSize,
		TCPBufInsNum:   Config.TCPBufInsNum,
		TCPBufioNum:    Config.TCPBufioNum,
	}

	c := make(chan chord.Notification)
	n, err := chord.Create(conf, c, serfLog)
	if err != nil {
		panic(err)
	}
	n.SetLogger(log)
	n.StartStatusTcp()

	go func() {
		for {
			notify := <-c
			if notify.Err != nil {
				log.Error("serf error %v: %s", notify.Err, notify.Msg)
			}
		}
	}()
}

func StartChordNode() error {
	go chordRoutine()
	return nil
}

func main() {
	var configFile string
	var printVer bool
	var entrypoint string
	var starthash string

	flag.BoolVar(&printVer, "version", false, "print version")
	flag.StringVar(&configFile, "c", "config.ini", "specify config file")
	flag.StringVar(&entrypoint, "e", "",
		"serf entrypoint used for joining into cluster")
	flag.StringVar(&starthash, "s", "", "chord node start hash hex string")

	flag.Parse()

	if printVer {
		dmq.PrintVersion()
		os.Exit(0)
	}

	if starthash == "" {
		fmt.Println("Warning: starthash must be provided!")
		flag.Usage()
		os.Exit(-1)
	}

	if err := InitConfig(configFile, entrypoint, starthash); err != nil {
		panic(err)
	}

	if err := dmq.ProcessInit(Config.Workdir, Config.PidFile); err != nil {
		panic(err)
	}

	if err := InitLog(Config.LogFile, Config.SerfLogFile); err != nil {
		panic(err)
	}

	if err := InitServer(); err != nil {
		panic(err)
	}

	if err := StartChordNode(); err != nil {
		panic(err)
	}

	for {
		time.Sleep(time.Second * time.Duration(10))
	}

	log.Info("Datanode stop")
}
