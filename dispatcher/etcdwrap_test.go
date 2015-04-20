package main

import (
	"fmt"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"testing"
	"time"
)

var ecpool *dmq.EtcdClientPool

func init() {
	cfg := fakeSrvConfig()
	ecpool = dmq.NewEtcdClientPool(cfg.EtcdMachines, 1, 16)
}

// FIXME: at present, if we want to run go test with this file, we must start
// a connector with hostname of 'conn0101' fisrt. In future work we will
// separate the connector into a standalone module in order to start a
// connector daemon in testing.

var connid string = "conn0101"

var matchport = 6000
var bindip = "10.0.10.25"

func fakeSrvConfig() *SrvConfig {
	cfg := &SrvConfig{
		NodeId:        "disp0101",
		BindIp:        bindip,
		MatchTCPBind:  "0.0.0.0:6000",
		MatchTCPPort:  matchport,
		HeartbeatIval: 30,
		EtcdMachines:  []string{"http://localhost:4001"},
	}
	return cfg
}

func TestRegisterEtcd(t *testing.T) {
	rmgr := &RouterManager{}
	cfg := fakeSrvConfig()
	err := RegisterEtcd(rmgr, cfg, ecpool)
	defer func() {
		UnregisterEtcd(cfg, ecpool)
		rmgr.conn.Close()
	}()
	if err != nil {
		t.Errorf("RegisterEtcd error(%v)", err)
	}
}

func TestGetConnRelatedDispAddr(t *testing.T) {
	// wait a short time for connector to re-register to etcd waiting conn list
	time.Sleep(time.Duration(100) * time.Millisecond)
	rmgr := &RouterManager{}
	cfg := fakeSrvConfig()
	if err := RegisterEtcd(rmgr, cfg, ecpool); err != nil {
		panic(err)
	}
	defer func() {
		UnregisterEtcd(cfg, ecpool)
		rmgr.conn.Close()
	}()

	if addr, err := GetConnRelatedDispAddr(connid, ecpool); err != nil {
		t.Errorf("Get Connector related Dispatcher failed: %v", err)
	} else if addr != fmt.Sprintf("%s:%d", bindip, matchport) {
		t.Errorf("wrong addr %s found, should be %s:%d", addr, bindip, matchport)
	}
}
