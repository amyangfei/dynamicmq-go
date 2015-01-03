package sdk

import (
	"errors"
	"fmt"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"net"
)

type SubSdk struct {
	// cliId is a 12-byte hex string as identity used in external system, among
	// clients using sdk. The system doesn't care about how it is generated. It
	// is reserved for external interface.
	cliId string
	token string
	conn  net.Conn
}

var (
	// mapping from command name to command content should be sent
	CmdTable = map[string]string{
		"subscribe": "subscribe",
		"heartbeat": "hb",
	}
)

func NewSubSdk() *SubSdk {
	// TODO: initial Token
	return &SubSdk{}
}

func (sub *SubSdk) sendMessage(msgs []string) error {
	sendMsg := make([]byte, 0)
	sendMsg = append(sendMsg, fmt.Sprintf("*%d%s", len(msgs), dmq.Crlf)...)
	for _, msg := range msgs {
		sendMsg = append(sendMsg, fmt.Sprintf("$%d%s", len(msg), dmq.Crlf)...)
		sendMsg = append(sendMsg, (msg + dmq.Crlf)...)
	}
	// add '#len\r\n'
	sendMsg = append(
		[]byte(fmt.Sprintf("#%d%s", len(sendMsg), dmq.Crlf)), sendMsg...)
	sent, err := sub.conn.Write(sendMsg)
	if sent != len(sendMsg) {
		return errors.New(
			fmt.Sprintf("send incomplete message should: %d sent: %d",
				len(sendMsg), sent))
	}
	return err
}

func (sub *SubSdk) Connect(dst string) error {
	addr, err := net.ResolveTCPAddr("tcp", dst)
	if err != nil {
		return err
	}
	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		return err
	}
	if err := conn.SetKeepAlive(true); err != nil {
		conn.Close()
		return err
	}
	sub.conn = conn
	return nil
}

func (sub *SubSdk) Auth() error {
	return nil
}

func (sub *SubSdk) Subscribe() error {
	return nil
}

func (sub *SubSdk) Heartbeat() error {
	if cmd, ok := CmdTable["heartbeat"]; !ok {
		return errors.New("heartbeat cmd not found")
	} else {
		if err := sub.sendMessage([]string{cmd}); err != nil {
			sub.Close()
			return err
		} else {
			resp := make([]byte, 64)
			sub.conn.Read(resp)
			return nil
		}
	}
}

func (sub *SubSdk) Close() error {
	return sub.conn.Close()
}
