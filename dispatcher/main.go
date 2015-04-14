package main

import (
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

var RouterMgr *RouterManager = &RouterManager{}

var EtcdCliPool *dmq.EtcdClientPool

var log = logging.MustGetLogger("dynamicmq-dispatcher")

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
			return
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

	serverFlagSet := flag.NewFlagSet("server", flag.PanicOnError)
	serverFlagSet.String("node_id", "disp0101", "server node id")
	serverFlagSet.String("bind_ip", "127.0.0.1", "bind ip of this dispatcher")
	serverFlagSet.Int("match_tcp_port", 6000, "tcp port for matching service")
	serverFlagSet.String("match_tcp_bind", "localhost:6000", "bind address for matcher")
	serverFlagSet.String("working_dir", ".", "working dir")
	serverFlagSet.String("log_level", "DEBUG", "log level")
	serverFlagSet.String("log_file", "./dispatcher.log", "log file path")
	serverFlagSet.String("pid_file", "./dispatcher_pid", "pid file")
	serverFlagSet.Int("tcp_recvbuf_size", 2048, "tcp receive buffer size")
	serverFlagSet.Int("tcp_sendbuf_size", 2048, "tcp send buffer size")
	serverFlagSet.Int("tcp_bufio_num", 64, "bufio num for each cache instance")
	serverFlagSet.Int("heartbeat_interval", 300, "heartbeat interval to connector-router")

	etcdFlagSet := flag.NewFlagSet("etcd", flag.PanicOnError)
	etcdFlagSet.String("machines", "http://localhost:4001", "etcd machines")
	etcdFlagSet.Int("pool_size", 4, "initial etcd client pool size")
	etcdFlagSet.Int("max_pool_size", 64, "max etcd client pool size")

	globalconf.Register("server", serverFlagSet)
	globalconf.Register("etcd", etcdFlagSet)

	conf.ParseAll()

	Config = &SrvConfig{}

	Config.NodeId = serverFlagSet.Lookup("node_id").Value.String()
	Config.BindIp = serverFlagSet.Lookup("bind_ip").Value.String()
	Config.MatchTCPPort, err =
		strconv.Atoi(serverFlagSet.Lookup("match_tcp_port").Value.String())
	Config.MatchTCPBind = fmt.Sprintf("0.0.0.0:%d", Config.MatchTCPPort)
	Config.WorkingDir = serverFlagSet.Lookup("working_dir").Value.String()
	Config.LogLevel = serverFlagSet.Lookup("log_level").Value.String()
	Config.LogFile = serverFlagSet.Lookup("log_file").Value.String()
	Config.PidFile = serverFlagSet.Lookup("pid_file").Value.String()
	Config.TCPRecvBufSize, err =
		strconv.Atoi(serverFlagSet.Lookup("tcp_recvbuf_size").Value.String())
	Config.TCPSendBufSize, err =
		strconv.Atoi(serverFlagSet.Lookup("tcp_sendbuf_size").Value.String())
	Config.TCPBufioNum, err =
		strconv.Atoi(serverFlagSet.Lookup("tcp_bufio_num").Value.String())
	Config.TCPBufInsNum = runtime.NumCPU()
	Config.HeartbeatIval, err =
		strconv.Atoi(serverFlagSet.Lookup("heartbeat_interval").Value.String())

	machines := etcdFlagSet.Lookup("machines").Value.String()
	Config.EtcdMachines = strings.Split(machines, ",")
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
	return nil
}

func InitServer() error {
	EtcdCliPool = dmq.NewEtcdClientPool(
		Config.EtcdMachines, Config.EtcdPoolSize, Config.EtcdPoolMaxSize)
	return nil
}

func ShutdownServer() {
	UnregisterEtcd(Config, EtcdCliPool)
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

	if err := dmq.ProcessInit(Config.WorkingDir, Config.PidFile); err != nil {
		panic(err)
	}

	if err := InitLog(Config.LogFile); err != nil {
		panic(err)
	}

	if err := InitServer(); err != nil {
		panic(err)
	}

	if err := StartMatchTCP(Config.MatchTCPBind); err != nil {
		panic(err)
	}

	// Errors may occur if this procedure acquires the waiting lock before the
	// expected connector registering to etcd. So try another time when error
	// occurs. We should ensure the RegisterEtcd can be called idempotently.
	if err := RegisterEtcd(RouterMgr, Config, EtcdCliPool); err != nil {
		log.Error("first time register to etcd with error: %v", err)
		time.Sleep(time.Second * time.Duration(1))
		if err2 := RegisterEtcd(RouterMgr, Config, EtcdCliPool); err2 != nil {
			panic(err2)
		}
	}

	signalChan := InitSignal()
	HandleSignal(signalChan)

	log.Info("connector stop")
}
