package main

import (
	"flag"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"github.com/op/go-logging"
	"github.com/rakyll/globalconf"
	"gopkg.in/mgo.v2/bson"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
)

var Config *SrvConfig

var SubcliTable map[bson.ObjectId]*SubClient

var EtcdCliPool *dmq.EtcdClientPool

var MetaRCPool *dmq.RedisCliPool

var AttrRCPool *dmq.RedisCliPool

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
	serverFlagSet.String("node_id", "conn0101", "server node id")
	serverFlagSet.String("bind_ip", "localhost", "server bind id")
	serverFlagSet.String("sub_tcp_bind", "localhost:7253", "bind address for subscriber")
	serverFlagSet.String("router_tcp_bind", "localhost:7255", "bind address for router")
	serverFlagSet.String("auth_srv_addr", "localhost:9000", "auth server address")
	serverFlagSet.String("working_dir", ".", "working dir")
	serverFlagSet.String("log_level", "DEBUG", "log level")
	serverFlagSet.String("log_file", "./connector.log", "log file path")
	serverFlagSet.String("pid_file", "./connector_pid", "pid file")
	serverFlagSet.Int("max_proc", 0, "max cpu process")
	serverFlagSet.Int("tcp_recvbuf_size", 2048, "tcp receive buffer size")
	serverFlagSet.Int("tcp_sendbuf_size", 2048, "tcp send buffer size")
	serverFlagSet.Int("tcp_bufio_num", 64, "bufio num for each cache instance")
	serverFlagSet.Int("sub_keepalive", 900, "keepalive timeout for subscriber")
	serverFlagSet.Int("disp_keepalive", 900, "keepalive timeout for dispatcher")
	serverFlagSet.Int("capacity", 100000, "subscriber capacity of this connector")

	redisFlagSet := flag.NewFlagSet("redis", flag.PanicOnError)
	redisFlagSet.String("meta_redis_addr", "localhost:6379", "meta redis bind address")
	redisFlagSet.String("attr_redis_addr", "localhost:6479", "attribute redis bind address")
	redisFlagSet.String("max_idle", "50", "redis pool max idle clients")
	redisFlagSet.String("max_active", "100", "redis pool max active clients")
	redisFlagSet.String("timeout", "3600", "close idle redis client after timeout")

	etcdFlagSet := flag.NewFlagSet("etcd", flag.PanicOnError)
	etcdFlagSet.String("machines", "http://localhost:4001", "etcd machines")
	etcdFlagSet.String("attr_machines", "http://localhost:4101", "etcd machines for attribute")
	etcdFlagSet.Int("pool_size", 4, "initial etcd client pool size")
	etcdFlagSet.Int("max_pool_size", 64, "max etcd client pool size")

	globalconf.Register("server", serverFlagSet)
	globalconf.Register("redis", redisFlagSet)
	globalconf.Register("etcd", etcdFlagSet)

	conf.ParseAll()

	Config = &SrvConfig{}

	Config.NodeId = serverFlagSet.Lookup("node_id").Value.String()
	Config.BindIp = serverFlagSet.Lookup("bind_ip").Value.String()
	Config.SubTCPBind = serverFlagSet.Lookup("sub_tcp_bind").Value.String()
	Config.RouterTCPBind = serverFlagSet.Lookup("router_tcp_bind").Value.String()
	Config.AuthSrvAddr = serverFlagSet.Lookup("auth_srv_addr").Value.String()
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
	Config.SubKeepalive, err =
		strconv.Atoi(serverFlagSet.Lookup("sub_keepalive").Value.String())
	Config.DispKeepalive, err =
		strconv.Atoi(serverFlagSet.Lookup("disp_keepalive").Value.String())
	Config.Capacity, err =
		strconv.Atoi(serverFlagSet.Lookup("capacity").Value.String())

	Config.MetaRedisAddr = redisFlagSet.Lookup("meta_redis_addr").Value.String()
	Config.AttrRedisAddr = redisFlagSet.Lookup("attr_redis_addr").Value.String()
	Config.RedisMaxIdle, err =
		strconv.Atoi(redisFlagSet.Lookup("max_idle").Value.String())
	Config.RedisMaxActive, err =
		strconv.Atoi(redisFlagSet.Lookup("max_active").Value.String())
	Config.RedisIdleTimeout, err =
		strconv.Atoi(redisFlagSet.Lookup("timeout").Value.String())

	machines := etcdFlagSet.Lookup("machines").Value.String()
	Config.EtcdMachines = strings.Split(machines, ",")
	Config.EtcdPoolSize, err =
		strconv.Atoi(etcdFlagSet.Lookup("pool_size").Value.String())
	Config.EtcdPoolMaxSize, err =
		strconv.Atoi(etcdFlagSet.Lookup("max_pool_size").Value.String())

	return nil
}

func InitLog(logFile, logLevel string) error {
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

func InitServer() error {
	var err error
	rcfg := dmq.NewRedisConfig(Config.MetaRedisAddr, Config.RedisMaxIdle,
		Config.RedisMaxActive, Config.RedisIdleTimeout)
	MetaRCPool, err = dmq.NewRedisCliPool(rcfg)
	if err != nil {
		return err
	}
	attrcfg := dmq.NewRedisConfig(Config.AttrRedisAddr, Config.RedisMaxIdle,
		Config.RedisMaxActive, Config.RedisIdleTimeout)
	AttrRCPool, err = dmq.NewRedisCliPool(attrcfg)
	if err != nil {
		return err
	}

	SubcliTable = make(map[bson.ObjectId]*SubClient, 0)

	EtcdCliPool = dmq.NewEtcdClientPool(
		Config.EtcdMachines, Config.EtcdPoolSize, Config.EtcdPoolMaxSize)

	return nil
}

func ShutdownServer() {
	if err := UnregisterEtcd(Config, EtcdCliPool); err != nil {
		panic(err)
	}
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

	if err := InitLog(Config.LogFile, Config.LogLevel); err != nil {
		panic(err)
	}

	if err := InitServer(); err != nil {
		panic(err)
	}

	if err := StartSubTCP(Config.SubTCPBind); err != nil {
		panic(err)
	}

	if err := StartRouter(Config.RouterTCPBind); err != nil {
		panic(err)
	}

	if err := RegisterEtcd(Config, EtcdCliPool); err != nil {
		panic(err)
	}

	signalChan := InitSignal()
	HandleSignal(signalChan)

	log.Info("connector stop")
}
