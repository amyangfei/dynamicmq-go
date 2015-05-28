package main

import (
	"crypto/sha1"
	"flag"
	"fmt"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"github.com/op/go-logging"
	"github.com/rakyll/globalconf"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// Server basic configuration
var Config *SrvConfig
var log = logging.MustGetLogger("dynamicmq-match-indexnode")

// Etcd client pool
var EtcdCliPool *dmq.EtcdClientPool

// Store attribute basic information including dimension, each dimension's info.
var IdxBase *IndexBase

// mapping from attribute-name-combination to its corresponding segment tree index
var AttrIdxesMap map[string]*AttrIndex

// Mapping from subclient's id to subclient information.
// The subclient's id is in BSON format, not hex string.
var ClisInfo map[string]*SubCliInfo

// Mapping from pnode's id to pnode, pnid is in plain string.
var PnodeMap map[string]*Pnode

// stores vnodes information
var Rtable *RTable

// mapping from Pnode's id to a TCP connection with it
var DnConns map[string]*DnodeConn

// InitSignal register signals handler.
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

func initConfig(configFile string) error {
	conf, err := globalconf.NewWithOptions(&globalconf.Options{
		Filename: configFile,
	})
	if err != nil {
		return err
	}

	basicFlagSet := flag.NewFlagSet("basic", flag.PanicOnError)
	basicFlagSet.String("node_id", "idxn0101", "index node server node_id")
	basicFlagSet.String("bind_ip", "127.0.0.1", "server bind ip")
	basicFlagSet.Int("pub_tcp_port", 6000, "message publishing tcp bind port")
	basicFlagSet.String("workdir", ".", "server working dir")
	basicFlagSet.String("log_level", "DEBUG", "log level")
	basicFlagSet.String("log_file", "./datanode.log", "log file path")
	basicFlagSet.String("pid_file", "./datanode_pid", "pid file")
	basicFlagSet.Int("tcp_recvbuf_size", 2048, "tcp receive buffer size")
	basicFlagSet.Int("tcp_sendbuf_size", 2048, "tcp send buffer size")
	basicFlagSet.Int("tcp_bufio_num", 64, "bufio num for each cache instance")
	basicFlagSet.Int("update_cache_thr", 100, "update cache threshold for attribute flush")
	basicFlagSet.Int("flush_interval", 60, "interval to run attribute flush, in second")
	basicFlagSet.Int("flush_timeout", 300, "if during timeout no attribute flush happened, force to run attribute flush")

	etcdFlagSet := flag.NewFlagSet("etcd", flag.PanicOnError)
	etcdFlagSet.String("machines", "http://localhost:4001", "etcd machine list")
	etcdFlagSet.String("attr_machines", "http://localhost:4101",
		"attr etcd machine list, same cluster is filtered by ',', different group is filtered by ';'")
	etcdFlagSet.Int("pool_size", 4, "initial etcd client pool size")
	etcdFlagSet.Int("max_pool_size", 64, "max etcd client pool size")

	redisFlagSet := flag.NewFlagSet("redis", flag.PanicOnError)
	redisFlagSet.String("attr_redis_addr", "tcp@localhost:6479",
		"attr redis address list. different group is filtered by ';'")
	redisFlagSet.String("max_idle", "50", "redis pool max idle clients")
	redisFlagSet.String("max_active", "100", "redis pool max active clients")
	redisFlagSet.String("timeout", "3600", "close idle redis client after timeout")

	globalconf.Register("basic", basicFlagSet)
	globalconf.Register("etcd", etcdFlagSet)
	globalconf.Register("redis", redisFlagSet)

	conf.ParseAll()

	Config = &SrvConfig{}

	Config.NodeID = basicFlagSet.Lookup("node_id").Value.String()
	Config.BindIP = basicFlagSet.Lookup("bind_ip").Value.String()
	pubTcpPort, err :=
		strconv.Atoi(basicFlagSet.Lookup("pub_tcp_port").Value.String())
	Config.PubTCPBind = fmt.Sprintf("0.0.0.0:%d", pubTcpPort)
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
	Config.UpdateCacheThr, err =
		strconv.Atoi(basicFlagSet.Lookup("update_cache_thr").Value.String())
	Config.FlushInterval, err =
		strconv.Atoi(basicFlagSet.Lookup("flush_interval").Value.String())
	Config.FlushTimeout, err =
		strconv.Atoi(basicFlagSet.Lookup("flush_timeout").Value.String())
	Config.HashFunc = sha1.New

	machines := etcdFlagSet.Lookup("machines").Value.String()
	Config.EtcdMachines = strings.Split(machines, ",")
	Config.EtcdPoolSize, err =
		strconv.Atoi(etcdFlagSet.Lookup("pool_size").Value.String())
	Config.EtcdPoolMaxSize, err =
		strconv.Atoi(etcdFlagSet.Lookup("max_pool_size").Value.String())

	attrAddrs := redisFlagSet.Lookup("attr_redis_addr").Value.String()
	Config.AttrRedisAddrs = strings.Split(attrAddrs, ";")
	Config.RedisMaxIdle, err =
		strconv.Atoi(redisFlagSet.Lookup("max_idle").Value.String())
	Config.RedisMaxActive, err =
		strconv.Atoi(redisFlagSet.Lookup("max_active").Value.String())
	Config.RedisIdleTimeout, err =
		strconv.Atoi(redisFlagSet.Lookup("timeout").Value.String())

	return nil
}

func initLog(logFile, logLevel string) error {
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

	return nil
}

func initServer() error {
	log.Info("Indexnode server is starting...")

	EtcdCliPool = dmq.NewEtcdClientPool(
		Config.EtcdMachines, Config.EtcdPoolSize, Config.EtcdPoolMaxSize)

	IdxBase = &IndexBase{}
	AttrIdxesMap = make(map[string]*AttrIndex)
	ClisInfo = make(map[string]*SubCliInfo)

	PnodeMap = make(map[string]*Pnode)
	Rtable = &RTable{
		vns: make([]*Vnode, 0),
	}

	DnConns = make(map[string]*DnodeConn)

	if err := initIndex(AttrIdxesMap, IdxBase, EtcdCliPool); err != nil {
		return err
	}
	return nil
}

func shutdownServer() {
	log.Info("Indexnode server stop...")
	os.Exit(0)
}

func notifyService() error {
	go dataNodeWatcher(Config.EtcdMachines)

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

func attrUpdateFlushService() {
	go func() {
		lastUpdate := time.Now().Unix()
		ticker := time.NewTicker(time.Second * time.Duration(Config.FlushInterval))
		for {
			<-ticker.C
			lastUpdate = processAttrUpdateFlush(lastUpdate)
		}
	}()
}

func main() {
	var configFile string
	var printVer bool

	flag.BoolVar(&printVer, "version", false, "print version")
	flag.StringVar(&configFile, "c", "config.ini", "specify config file")
	flag.Parse()

	if printVer {
		dmq.PrintVersion()
		os.Exit(0)
	}

	if err := initConfig(configFile); err != nil {
		panic(err)
	}

	if err := dmq.ProcessInit(Config.Workdir, Config.PidFile); err != nil {
		panic(err)
	}

	if err := initLog(Config.LogFile, Config.LogLevel); err != nil {
		panic(err)
	}

	if err := initServer(); err != nil {
		panic(err)
	}

	if err := notifyService(); err != nil {
		panic(err)
	}

	attrUpdateFlushService()

	startPubTCP(Config.PubTCPBind)

	signalChan := initSignal()
	handleSignal(signalChan)
}
