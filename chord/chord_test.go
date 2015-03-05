package chord

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"
)

func fastConf() *NodeConfig {
	conf := DefaultConfig("test")
	conf.serf.BinPath = "/usr/local/bin/serf"
	conf.serf.Args = []string{"-log-level=debug"}
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
		c <- Notification{err: err, msg: out.String()}
		return
	}
	if err := cmd.Wait(); err != nil {
		c <- Notification{err: err, msg: out.String()}
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
		c <- Notification{err: err, msg: out.String()}
	}
	c <- Notification{err: nil, msg: out.String()}
}

func TestCreateShutdown(t *testing.T) {
	conf := fastConf()
	c := make(chan Notification)
	n, err := Create(conf, c)
	if err != nil {
		t.Errorf("failed to create node %v", err)
	}

	// monitor notification channel
	go func() {
		for {
			notify := <-c
			if notify.err != nil {
				// only record the error log from serf
				t.Logf("serf minor error %v: %s", notify.err, notify.msg)
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

func TestJoin(t *testing.T) {
	conf := fastConf()
	c := make(chan Notification)
	n, err := Create(conf, c)
	if err != nil {
		t.Errorf("failed to create node %v", err)
	}
	go fakeSerf(c)

	// monitor notification channel
	go func() {
		for {
			notify := <-c
			if notify.err != nil {
				t.Logf("serf minor error %v: %s", notify.err, notify.msg)
			}
		}
	}()

	// FIXME: short time wait for serf agent startup
	time.Sleep(time.Millisecond * time.Duration(100))
	if err := n.serfJoin(fakeSerfBind); err != nil {
		t.Errorf("serf join error: %v", err)
	}

	if n != nil {
		err := n.Shutdown()
		if err != nil {
			t.Errorf("node shutdown error: %v", err)
		}
	}
	go fakeSerfLeave(c)
}
