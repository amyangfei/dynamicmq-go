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

var connid = "conn0101"

var matchport = 6000
var bindip = "10.0.10.25"

func fakeSrvConfig() *SrvConfig {
	cfg := &SrvConfig{
		NodeID:        "disp0101",
		BindIP:        bindip,
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
	err := registerEtcd(rmgr, cfg, ecpool)
	defer func() {
		unRegisterEtcd(cfg, ecpool)
		rmgr.conn.Close()
	}()
	if err != nil {
		t.Errorf("registerEtcd error(%v)", err)
	}
}

func TestGetConnRelatedDispAddr(t *testing.T) {
	// wait a short time for connector to re-register to etcd waiting conn list
	time.Sleep(time.Duration(100) * time.Millisecond)
	rmgr := &RouterManager{}
	cfg := fakeSrvConfig()
	if err := registerEtcd(rmgr, cfg, ecpool); err != nil {
		panic(err)
	}
	defer func() {
		unRegisterEtcd(cfg, ecpool)
		rmgr.conn.Close()
	}()

	info, err := getConnRelatedDispInfo(connid, ecpool)
	if err != nil {
		t.Errorf("Get Connector related info failed: %v", err)
	}
	addr, ok := info["addr"]
	if !ok {
		t.Errorf("addr not in info: %v", info)
	}
	if addr != fmt.Sprintf("%s:%d", bindip, matchport) {
		t.Errorf("wrong addr %s found, should be %s:%d", addr, bindip, matchport)
	}
}
