package chord

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"os/exec"
)

func checkMemberAlive(serfBinPath, serfRPCAddr string) (string, error) {
	var args []string
	args = append(args, "members")
	args = append(args, fmt.Sprintf("-rpc-addr=%s", serfRPCAddr))

	cmd := exec.Command(serfBinPath, args...)
	cmd.Env = os.Environ()[:]
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out

	err := cmd.Run()
	return out.String(), err
}

func serfStart(c chan Notification, logger io.Writer, serfBinPath string, params map[string]string, otherArgs []string) {
	var args []string
	args = append(args, "agent")
	for k, v := range params {
		args = append(args, fmt.Sprintf("-%s=%s", k, v))
	}
	if len(otherArgs) > 0 {
		args = append(args, otherArgs...)
	}

	cmd := exec.Command(serfBinPath, args...)
	cmd.Env = os.Environ()[:]
	var out bytes.Buffer
	cmd.Stdout = logger
	cmd.Stderr = &out

	go func() {
		if err := cmd.Run(); err != nil {
			c <- Notification{Err: err, Msg: out.String()}
		}
	}()
}

func serfStop(serfBinPath, serfRPCAddr string) error {
	var args []string
	args = append(args, "leave")
	if serfRPCAddr != "" {
		args = append(args, fmt.Sprintf("-rpc-addr=%s", serfRPCAddr))
	}

	cmd := exec.Command(serfBinPath, args...)
	cmd.Env = os.Environ()[:]
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out

	if err := cmd.Run(); err != nil {
		// "client closed" error may occur, ignore it
	}

	return nil
}

// Call This function to tell an serf agent to join the cluster
// addr: an arbitrary serf bind address of nodes in Chord ring
// serfRPCAddr: the rpc address of this serf agent
func serfJoin(serfBinPath, serfRPCAddr, addr string) error {
	var args []string
	args = append(args, "join")
	if serfRPCAddr != "" {
		args = append(args, fmt.Sprintf("-rpc-addr=%s", serfRPCAddr))
	}
	args = append(args, addr)

	cmd := exec.Command(serfBinPath, args...)
	cmd.Env = os.Environ()[:]
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("%v: %s", err, out.String())
	}

	return nil
}

// Call This function to send serf user event to serf cluster
// evname, represents for event type, including:
// 	 nodeinfo: information of chord physical node
// 	 vnodeinfo: information of all vnodes that belongs to one chord physical node
// coalesce:
//   If coalesce is true, if many events of the same name are received within a
//   short amount of time, the event handler is only invoked once.
func serfUserEvent(serfBinPath, evname, payload string, params map[string]string, c chan Notification) {
	var args []string
	args = append(args, "event")
	for k, v := range params {
		args = append(args, fmt.Sprintf("-%s=%s", k, v))
	}
	args = append(args, evname)
	args = append(args, payload)

	cmd := exec.Command(serfBinPath, args...)
	cmd.Env = os.Environ()[:]
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out

	if err := cmd.Run(); err != nil {
		go func() {
			c <- Notification{Err: err, Msg: out.String()}
		}()
	}
}
