package chord

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"
)

var serfLogFile = "./serfagent.log"

func fastConf() *NodeConfig {
	conf := DefaultConfig("localhost", "serf0101")
	conf.WorkDir = "."
	conf.Serf.Args = []string{"-log-level=info"}
	return conf
}

var fakeSerfBind string = "127.0.0.1:7497"
var fakeSerfRPC string = "127.0.0.1:7374"

func fakeSerfConf() *SerfConfig {
	return &SerfConfig{
		NodeName: "fakeserf",
		BinPath:  "/usr/local/bin/serf",
		BindAddr: "0.0.0.0:7497",
		RPCAddr:  fakeSerfRPC,
	}
}

func fakeSerf(c chan Notification) {
	conf := fakeSerfConf()

	args := make([]string, 0)
	args = append(args, "agent")
	args = append(args, fmt.Sprintf("-node=%s", conf.NodeName))
	args = append(args, fmt.Sprintf("-bind=%s", conf.BindAddr))
	args = append(args, fmt.Sprintf("-rpc-addr=%s", conf.RPCAddr))
	args = append(args, conf.Args...)

	cmd := exec.Command(conf.BinPath, args...)
	cmd.Env = os.Environ()[:]
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out

	if err := cmd.Start(); err != nil {
		c <- Notification{Err: err, Msg: out.String()}
		return
	}
	if err := cmd.Wait(); err != nil {
		c <- Notification{Err: err, Msg: out.String()}
	}
}

func fakeSerfLeave(c chan Notification) {
	conf := fakeSerfConf()

	args := make([]string, 0)
	args = append(args, "leave")
	args = append(args, fmt.Sprintf("-rpc-addr=%s", conf.RPCAddr))

	cmd := exec.Command(conf.BinPath, args...)
	cmd.Env = os.Environ()[:]
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out

	if err := cmd.Run(); err != nil {
		c <- Notification{Err: err, Msg: out.String()}
	}
	c <- Notification{Err: nil, Msg: out.String()}
}

func TestCreateShutdown(t *testing.T) {
	conf := fastConf()
	c := make(chan Notification)
	logger, err := os.OpenFile(serfLogFile, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		t.Errorf("failed to open file %s with error: %v", serfLogFile, err)
		return
	}
	n, err := Create(conf, c, logger)
	if err != nil {
		t.Errorf("failed to create node %v", err)
	}

	// monitor notification channel
	go func() {
		for {
			notify := <-c
			if notify.Err != nil {
				// only record the error log from serf
				t.Logf("serf minor error %v: %s", notify.Err, notify.Msg)
			}
		}
	}()

	time.Sleep(time.Millisecond * time.Duration(100))
	if n != nil {
		err := n.Shutdown()
		if err != nil {
			t.Errorf("node shutdown error: %v", err)
		}
	}
}

func TestLifeCycle(t *testing.T) {
	conf := fastConf()
	c := make(chan Notification)
	logger, err := os.OpenFile(serfLogFile, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		t.Errorf("failed to open file %s with error: %v", serfLogFile, err)
		return
	}
	n, err := Create(conf, c, logger)
	if err != nil {
		t.Errorf("failed to create node %v", err)
	}
	go fakeSerf(c)

	// monitor notification channel
	go func() {
		for {
			notify := <-c
			if notify.Err != nil {
				t.Logf("serf minor error %v: %s", notify.Err, notify.Msg)
			}
		}
	}()

	// FIXME: short time wait for serf agent startup
	time.Sleep(time.Millisecond * time.Duration(100))
	if err := n.serfJoin(fakeSerfBind); err != nil {
		t.Errorf("serf join error: %v", err)
	}

	n.serfUserEvent("nodeinfo", "testpayload", false, c)
	time.Sleep(time.Millisecond * time.Duration(100))

	if n != nil {
		err := n.Shutdown()
		if err != nil {
			t.Errorf("node shutdown error: %v", err)
		}
	}
	go fakeSerfLeave(c)
}
