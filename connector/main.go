package main

import (
	// log "github.com/op/go-logging"
	"flag"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"github.com/op/go-logging"
	"github.com/rakyll/globalconf"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"syscall"
)

var Config *SrvConfig

var SubcliTable map[string]*SubClient

var log = logging.MustGetLogger("dynamicmq-connector")

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
	serverFlagSet.String("node_id", "conn-01-01", "server node id")
	serverFlagSet.String("sub_tcp_bind", "localhost:7253", "bind address for subscriber")
	serverFlagSet.String("router_tcp_bind", "localhost:7255", "bind address for router")
	serverFlagSet.String("working_dir", ".", "working dir")
	serverFlagSet.String("log_level", "DEBUG", "log level")
	serverFlagSet.String("log_file", "./connector.log", "log file path")
	serverFlagSet.String("pid_file", "./connector_pid", "pid file")
	serverFlagSet.Int("max_proc", 0, "max cpu process")
	serverFlagSet.Int("tcp_recvbuf_size", 2048, "tcp receive buffer size")
	serverFlagSet.Int("tcp_sendbuf_size", 2048, "tcp send buffer size")
	serverFlagSet.Int("tcp_bufio_num", 64, "bufio num for each cache instance")

	redisFlagSet := flag.NewFlagSet("redis", flag.PanicOnError)
	redisFlagSet.String("redis_endpoint", "localhost:6379", "redis endpoint")
	redisFlagSet.String("max_idle", "50", "redis pool max idle clients")
	redisFlagSet.String("max_active", "100", "redis pool max active clients")
	redisFlagSet.String("timeout", "3600", "close idle redis client after timeout")

	zkFlagSet := flag.NewFlagSet("zookeeper", flag.PanicOnError)
	zkFlagSet.String("addr", "localhost:2181", "zookeeper host")

	globalconf.Register("server", serverFlagSet)
	globalconf.Register("redis", redisFlagSet)
	globalconf.Register("zookeeper", zkFlagSet)

	conf.ParseAll()

	Config = &SrvConfig{}

	Config.NodeId = serverFlagSet.Lookup("node_id").Value.String()
	Config.SubTCPBind = serverFlagSet.Lookup("sub_tcp_bind").Value.String()
	Config.RouterTCPBind = serverFlagSet.Lookup("router_tcp_bind").Value.String()
	Config.WorkingDir = serverFlagSet.Lookup("working_dir").Value.String()
	Config.LogLevel = serverFlagSet.Lookup("log_level").Value.String()
	Config.LogFile = serverFlagSet.Lookup("log_file").Value.String()
	Config.PidFile = serverFlagSet.Lookup("pid_file").Value.String()
	Config.MaxProc, err =
		strconv.Atoi(serverFlagSet.Lookup("max_proc").Value.String())
	Config.TCPRecvBufSize, err =
		strconv.Atoi(serverFlagSet.Lookup("tcp_recvbuf_size").Value.String())
	Config.TCPSendBufSize, err =
		strconv.Atoi(serverFlagSet.Lookup("tcp_sendbuf_size").Value.String())
	Config.TCPBufioNum, err =
		strconv.Atoi(serverFlagSet.Lookup("tcp_bufio_num").Value.String())
	Config.TCPBufInsNum = runtime.NumCPU()

	Config.RedisEndPoint = redisFlagSet.Lookup("redis_endpoint").Value.String()
	Config.RedisMaxIdle, err =
		strconv.Atoi(redisFlagSet.Lookup("max_idle").Value.String())
	Config.RedisMaxActive, err =
		strconv.Atoi(redisFlagSet.Lookup("max_active").Value.String())
	Config.RedisIdleTimeout, err =
		strconv.Atoi(redisFlagSet.Lookup("timeout").Value.String())

	Config.ZookeeperAddr = zkFlagSet.Lookup("addr").Value.String()

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
	SubcliTable = make(map[string]*SubClient, 0)
	return nil
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
	if err := InitLog(Config.LogFile); err != nil {
		panic(err)
	}

	if err := InitServer(); err != nil {
		panic(err)
	}

	if err := InitRawMsgCache(); err != nil {
		panic(err)
	}

	if err := StartSubTCP(Config.SubTCPBind); err != nil {
		panic(err)
	}

	if err := StartRouter(Config.RouterTCPBind); err != nil {
		panic(err)
	}

	signalChan := InitSignal()
	HandleSignal(signalChan)

	log.Info("connector stop")
}
