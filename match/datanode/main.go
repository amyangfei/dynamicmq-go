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
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
)

// Server basic configuration
var Config *SrvConfig

// Etcd client pool
var EtcdCliPool *dmq.EtcdClientPool

// Meta redis client pool
var MetaRCPool *dmq.RedisCliPool

// common log
var log = logging.MustGetLogger("dynamicmq-match-datanode")

// logger used for serf daemon in chord node
var serfLog *os.File

// Node in chord topology
var ChordNode *chord.Node

// Mapping from subclient's id to subclient information
// The subclient's id is in BSON format, not hex string
var ClisInfo = dmq.NewConcurrentMap()

// mapping from dispatcher's id(disp name) to a DispConn struct with it
var DispConns map[string]*DispConn

// the connected dispatcher at present
var CurDispNode *DispNode

// initSignal register signals handler.
func initSignal() chan os.Signal {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM,
		syscall.SIGINT, syscall.SIGSTOP)
	return c
}

func handleSignal(c chan os.Signal) {
	// Block until a signal is received.
	for {
		s := <-c
		log.Info("get a signal %s", s.String())
		switch s {
		case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGSTOP, syscall.SIGINT:
			shutdownServer()
		case syscall.SIGHUP:
			// TODO reload
		default:
			return
		}
	}
}

func initConfig(configFile, entrypoint, starthash string) error {
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

	serfFlagSet := flag.NewFlagSet("serf", flag.PanicOnError)
	serfFlagSet.String("bin_path", "/usr/local/bin/serf", "serf bin path")
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

	etcdFlagSet := flag.NewFlagSet("etcd", flag.PanicOnError)
	etcdFlagSet.String("machines", "http://localhost:4001", "etcd machines")
	etcdFlagSet.Int("pool_size", 4, "initial etcd client pool size")
	etcdFlagSet.Int("max_pool_size", 64, "max etcd client pool size")

	redisFlagSet := flag.NewFlagSet("redis", flag.PanicOnError)
	redisFlagSet.String("meta_redis_addr", "tcp@localhost:6379", "meta redis address")
	redisFlagSet.String("attr_redis_addr", "tcp@localhost:6479",
		"attr redis address list. different group is filtered by ';'")
	redisFlagSet.String("max_idle", "50", "redis pool max idle clients")
	redisFlagSet.String("max_active", "100", "redis pool max active clients")
	redisFlagSet.String("timeout", "3600", "close idle redis client after timeout")

	globalconf.Register("basic", basicFlagSet)
	globalconf.Register("serf", serfFlagSet)
	globalconf.Register("chord", chordFlagSet)
	globalconf.Register("etcd", etcdFlagSet)
	globalconf.Register("redis", redisFlagSet)

	conf.ParseAll()

	Config = &SrvConfig{}

	Config.BindIP = basicFlagSet.Lookup("bind_ip").Value.String()
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
	Config.HashFunc = sha1.New

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
	Config.BindAddr = fmt.Sprintf("%s:%d", Config.BindIP, Config.BindPort)
	Config.RPCPort, err =
		strconv.Atoi(chordFlagSet.Lookup("rpc_port").Value.String())
	Config.RPCAddr = fmt.Sprintf("%s:%d", Config.BindIP, Config.RPCPort)
	Config.NumVnodes, err =
		strconv.Atoi(chordFlagSet.Lookup("num_vnodes").Value.String())
	Config.NumSuccessors, err =
		strconv.Atoi(chordFlagSet.Lookup("num_successors").Value.String())
	Config.HashBits, err =
		strconv.Atoi(chordFlagSet.Lookup("hash_bits").Value.String())

	machines := etcdFlagSet.Lookup("machines").Value.String()
	Config.EtcdMachines = strings.Split(machines, ",")
	Config.EtcdPoolSize, err =
		strconv.Atoi(etcdFlagSet.Lookup("pool_size").Value.String())
	Config.EtcdPoolMaxSize, err =
		strconv.Atoi(etcdFlagSet.Lookup("max_pool_size").Value.String())

	Config.MetaRedisAddr = redisFlagSet.Lookup("meta_redis_addr").Value.String()
	attrAddrs := redisFlagSet.Lookup("attr_redis_addr").Value.String()
	Config.AttrRedisAddrs = strings.Split(attrAddrs, ";")
	Config.RedisMaxIdle, err =
		strconv.Atoi(redisFlagSet.Lookup("max_idle").Value.String())
	Config.RedisMaxActive, err =
		strconv.Atoi(redisFlagSet.Lookup("max_active").Value.String())
	Config.RedisIdleTimeout, err =
		strconv.Atoi(redisFlagSet.Lookup("timeout").Value.String())

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

func initLog(logFile, serfLogFile, logLevel string) error {
	var format = logging.MustStringFormatter(
		"%{time:2006-01-02 15:04:05.000} [%{level:.4s}] %{id:03x} [%{shortfunc}] %{message}",
	)

	f, err := os.OpenFile(logFile, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	backend1 := logging.NewLogBackend(f, "", 0)
	backend1Formatter := logging.NewBackendFormatter(backend1, format)
	backend1Leveled := logging.AddModuleLevel(backend1Formatter)
	backend1Leveled.SetLevel(dmq.LogLevelMap[logLevel], "")
	logging.SetBackend(backend1Leveled)

	serfLog, err = os.OpenFile(serfLogFile, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}

	return nil
}

func initServer() error {
	log.Info("Datanode server is starting...")
	EtcdCliPool = dmq.NewEtcdClientPool(
		Config.EtcdMachines, Config.EtcdPoolSize, Config.EtcdPoolMaxSize)

	DispConns = make(map[string]*DispConn)
	var err error
	CurDispNode, err = allocateDispNode(EtcdCliPool)
	if err != nil {
		return err
	}

	rcfg := dmq.NewRedisConfig(Config.MetaRedisAddr, Config.RedisMaxIdle,
		Config.RedisMaxActive, Config.RedisIdleTimeout)
	MetaRCPool, err = dmq.NewRedisCliPool(rcfg)
	if err != nil {
		return err
	}

	return nil
}

func shutdownServer() {
	log.Info("Datanode stop...")
	unRegisterDN(Config, ChordNode, EtcdCliPool)
	os.Exit(0)
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
		HostIP:         Config.BindIP,
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
	var err error
	ChordNode, err = chord.Create(conf)
	if err != nil {
		panic(err)
	}
	ChordNode.SetLogger(log)
	ChordNode.StartStatusTcp()
	ChordNode.SerfSchdule(c, serfLog)

	// FIXME: register datanode and vnode in a more accruacy time
	if err := registerDataNode(Config, EtcdCliPool); err != nil {
		panic(err)
	}
	if err := registerVnodes(Config, ChordNode, EtcdCliPool); err != nil {
		panic(err)
	}

	go func() {
		for {
			notify := <-c
			if notify.Err != nil {
				log.Error("serf error %v: %s", notify.Err, notify.Msg)
			}
		}
	}()
}

func startChordNode() error {
	go chordRoutine()
	return nil
}

func notifyService() error {
	for _, attrRedisAddr := range Config.AttrRedisAddrs {
		rcfg := dmq.NewRedisConfig(attrRedisAddr, Config.RedisMaxIdle,
			Config.RedisMaxActive, Config.RedisIdleTimeout)
		rcpool, err := dmq.NewRedisCliPool(rcfg)
		if err != nil {
			return err
		}

		go attrWatcher(rcpool)
	}
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

	if err := initConfig(configFile, entrypoint, starthash); err != nil {
		panic(err)
	}

	if err := dmq.ProcessInit(Config.Workdir, Config.PidFile); err != nil {
		panic(err)
	}

	if err := initLog(Config.LogFile, Config.SerfLogFile, Config.LogLevel); err != nil {
		panic(err)
	}

	if err := initServer(); err != nil {
		panic(err)
	}

	if err := startChordNode(); err != nil {
		panic(err)
	}

	if err := notifyService(); err != nil {
		panic(err)
	}

	startPubTCP(Config.BindAddr)

	signalChan := initSignal()
	handleSignal(signalChan)
}
