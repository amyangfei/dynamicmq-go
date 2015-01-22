package main

import (
	"testing"
)

// FIXME: at present, if we want to run go test with this file, we must start
// a connector fisrt. In future work we will separate the connector into a
// standalone module in order to start a connector daemon in testing.

func fakeSrvConfig() *SrvConfig {
	cfg := &SrvConfig{
		NodeId:        "disp0101",
		MatchTCPBind:  "0.0.0.0:6000",
		HeartbeatIval: 30,
		EtcdMachiens:  []string{"http://localhost:4001"},
	}
	return cfg
}

func TestRegisterEtcd(t *testing.T) {
	rmgr := &RouterManager{}
	cfg := fakeSrvConfig()
	err := RegisterEtcd(rmgr, cfg)
	defer UnregisterEtcd(cfg)
	if err != nil {
		t.Errorf("RegisterEtcd error(%v)", err)
	}
}
