package main

import (
	"flag"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"github.com/op/go-logging"
	"github.com/rakyll/globalconf"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

var Config *SrvConfig

var log = logging.MustGetLogger("dynamicmq-authsrv")

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
	serverFlagSet.String("node_id", "authsrv-01-01", "server node id")
	serverFlagSet.String("sub_auth_tcp_bind", "localhost:9000", "bind address for subscriber auth")
	serverFlagSet.String("pub_auth_tcp_bind", "localhost:9100", "bind address for publisher auth")
	serverFlagSet.String("working_dir", ".", "working dir")
	serverFlagSet.String("log_level", "DEBUG", "log level")
	serverFlagSet.String("log_file", "./authsrv.log", "log file path")
	serverFlagSet.String("pid_file", "./authsrv.pid", "pid file")
	serverFlagSet.String("sign_key", "a random signature salt", "signature salt")

	etcdFlagSet := flag.NewFlagSet("etcd", flag.PanicOnError)
	etcdFlagSet.String("machines", "http://localhost:4001", "etcd machines")

	globalconf.Register("server", serverFlagSet)
	globalconf.Register("etcd", etcdFlagSet)

	conf.ParseAll()

	Config = &SrvConfig{}
	Config.NodeId = serverFlagSet.Lookup("node_id").Value.String()
	Config.SubAuthTCPBind = serverFlagSet.Lookup("sub_auth_tcp_bind").Value.String()
	Config.PubAuthTCPBind = serverFlagSet.Lookup("pub_auth_tcp_bind").Value.String()
	Config.WorkingDir = serverFlagSet.Lookup("working_dir").Value.String()
	Config.LogLevel = serverFlagSet.Lookup("log_level").Value.String()
	Config.LogFile = serverFlagSet.Lookup("log_file").Value.String()
	Config.PidFile = serverFlagSet.Lookup("pid_file").Value.String()
	Config.SignKey = serverFlagSet.Lookup("sign_key").Value.String()

	machines := etcdFlagSet.Lookup("machines").Value.String()
	Config.EtcdMachiens = strings.Split(machines, ";")

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
	rand.Seed(time.Now().UnixNano())
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

	go StartServer(Config.SubAuthTCPBind)

	signalChan := InitSignal()
	HandleSignal(signalChan)

	log.Info("authsrv stop")
}
