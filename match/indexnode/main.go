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
)

var Config *SrvConfig
var log = logging.MustGetLogger("dynamicmq-match-indexnode")

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

	etcdFlagSet := flag.NewFlagSet("etcd", flag.PanicOnError)
	etcdFlagSet.String("machines", "http://localhost:4001", "etcd machine list")

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

	machines := etcdFlagSet.Lookup("machines").Value.String()
	Config.EtcdMachines = strings.Split(machines, ",")

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
	return nil
}

func ShutdownServer() {
	log.Info("Indexnode stop...")
	os.Exit(0)
}

func NotifyService() {
	go AttrWatcher(Config.EtcdMachines)
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

	signalChan := InitSignal()
	HandleSignal(signalChan)
}