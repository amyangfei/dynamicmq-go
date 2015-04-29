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
func InitSignal() chan os.Signal {
	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM,
		syscall.SIGINT, syscall.SIGSTOP)
	return c
}

func HandleSignal(c chan os.Signal) {
	// Block until a signal is received.
	for {
		s := <-c
		log.Info("get a signal %s", s.String())
		switch s {
		case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGSTOP, syscall.SIGINT:
			ShutdownServer()
		case syscall.SIGHUP:
			// TODO reload
		default:
			return
		}
	}
}

func InitConfig(configFile string) error {
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

	globalconf.Register("basic", basicFlagSet)
	globalconf.Register("etcd", etcdFlagSet)

	conf.ParseAll()

	Config = &SrvConfig{}

	Config.NodeId = basicFlagSet.Lookup("node_id").Value.String()
	Config.BindIp = basicFlagSet.Lookup("bind_ip").Value.String()
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
	etcdMachGroup := strings.Split(etcdFlagSet.Lookup("attr_machines").Value.String(), ";")
	attrEtcdMach := make([][]string, 0)
	for _, emg := range etcdMachGroup {
		attrEtcdMach = append(attrEtcdMach, strings.Split(emg, ";"))
	}
	Config.AttrEtcdMachines = attrEtcdMach

	Config.EtcdPoolSize, err =
		strconv.Atoi(etcdFlagSet.Lookup("pool_size").Value.String())
	Config.EtcdPoolMaxSize, err =
		strconv.Atoi(etcdFlagSet.Lookup("max_pool_size").Value.String())

	return nil
}

func InitLog(logFile string) error {
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
	logging.SetLevel(logging.DEBUG, "dynamicmq-match-indexnode")

	return nil
}

func InitServer() error {
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

	if err := InitIndex(AttrIdxesMap, IdxBase, EtcdCliPool); err != nil {
		return err
	}
	return nil
}

func ShutdownServer() {
	log.Info("Indexnode server stop...")
	os.Exit(0)
}

func NotifyService() {
	for _, machines := range Config.AttrEtcdMachines {
		go AttrWatcher(machines)
	}
	go DataNodeWatcher(Config.EtcdMachines)
}

func AttrUpdateFlushService() {
	go func() {
		lastUpdate := time.Now().Unix()
		ticker := time.NewTicker(time.Second * time.Duration(Config.FlushInterval))
		for {
			<-ticker.C
			lastUpdate = ProcessAttrUpdateFlush(lastUpdate)
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

	if err := InitConfig(configFile); err != nil {
		panic(err)
	}

	if err := dmq.ProcessInit(Config.Workdir, Config.PidFile); err != nil {
		panic(err)
	}

	if err := InitLog(Config.LogFile); err != nil {
		panic(err)
	}

	if err := InitServer(); err != nil {
		panic(err)
	}

	NotifyService()

	AttrUpdateFlushService()

	StartPubTCP(Config.PubTCPBind)

	signalChan := InitSignal()
	HandleSignal(signalChan)
}
