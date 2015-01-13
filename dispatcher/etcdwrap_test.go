package main

import (
	"testing"
)

func fakeSrvConfig() *SrvConfig {
	cfg := &SrvConfig{
		NodeId:       "disp0101",
		MatchTCPBind: "0.0.0.0:6000",
		EtcdMachiens: []string{"http://localhost:4001"},
	}
	return cfg
}

func TestRegisterEtcd(t *testing.T) {
	cfg := fakeSrvConfig()
	err := RegisterEtcd(cfg)
	defer UnregisterEtcd(cfg)
	if err != nil {
		t.Errorf("RegisterEtcd error(%v)", err)
	}
}
