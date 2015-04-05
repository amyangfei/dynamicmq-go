package chord

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"testing"
	"time"
)

var serfBinPath = "/usr/local/bin/serf"
var serfBindAddr = "0.0.0.0:12000"
var serfRPCAddr = "127.0.0.1:13000"

func startSerfAgent(t *testing.T) {
	args := make([]string, 0)
	args = append(args, "agent")
	args = append(args, fmt.Sprintf("-bind=%s", serfBindAddr))
	args = append(args, fmt.Sprintf("-rpc-addr=%s", serfRPCAddr))

	cmd := exec.Command(serfBinPath, args...)
	cmd.Env = os.Environ()[:]
	var out bytes.Buffer
	// cmd.Stdout = &out
	cmd.Stderr = &out

	go func() {
		if err := cmd.Run(); err != nil {
			t.Logf("serf agent error %v: %s", err, out.String())
		}
	}()
}

func TestCheckMemberAlive(t *testing.T) {
	startSerfAgent(t)
	tsleep := 100
	time.Sleep(time.Millisecond * time.Duration(tsleep))
	msg, err := checkMemberAlive(serfBinPath, serfRPCAddr)
	t.Logf("msg: %s err: %v", msg, err)

	serfStop(serfBinPath, serfRPCAddr)
}
