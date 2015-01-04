package sdk

import (
	"encoding/json"
	"errors"
	"fmt"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"io/ioutil"
	"net"
	"net/http"
	"regexp"
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
		"auth":      "auth",
		"subscribe": "sub",
		"heartbeat": "hb",
	}
)

func validSubClientId(cliId string) bool {
	r, _ := regexp.Compile("^[0-9a-fA-F]{24}$")
	return r.MatchString(cliId)
}

func NewSubSdk(cliId string) (*SubSdk, error) {
	if !validSubClientId(cliId) {
		return nil, errors.New("invalid sub client id")
	}
	return &SubSdk{cliId: cliId}, nil
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

func (sub *SubSdk) connect(dst string) error {
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

func (sub *SubSdk) getPreAuthHost() (string, error) {
	// TODO: get preauth addr from global config service like etcd
	preAuthHost := "localhost:9000"
	return preAuthHost, nil
}

func (sub *SubSdk) preAuth() (map[string]string, error) {
	preAuthHost, err := sub.getPreAuthHost()
	if err != nil {
		return nil, err
	}

	preAuthUrl := fmt.Sprintf("http://%s/sdk/sub/preauth?client_id=%s", preAuthHost, sub.cliId)
	resp, err := http.Get(preAuthUrl)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	data := make(map[string]string)
	if err := json.Unmarshal(body, &data); err != nil {
		return nil, err
	}
	if status, ok := data["status"]; !ok {
		return nil, errors.New("status field not in prepare response")
	} else if status != "ok" {
		return nil, errors.New("preauth failed")
	}

	if _, ok := data["timestamp"]; !ok {
		return nil, errors.New("timestamp field not in prepare response")
	}
	if _, ok := data["token"]; !ok {
		return nil, errors.New("token field not in prepare response")
	}
	if _, ok := data["connector"]; !ok {
		return nil, errors.New("connector field not in prepare response")
	}

	return data, nil
}

func (sub *SubSdk) prepareAuthCmd(data map[string]string) ([]byte, error) {
	timestamp, _ := data["timestamp"]
	token, _ := data["token"]

	cmdData := map[string]string{
		"client_id": sub.cliId,
		"timestamp": timestamp,
		"token":     token,
	}
	if jsonData, err := json.Marshal(cmdData); err != nil {
		return nil, err
	} else {
		return jsonData, nil
	}
}

func (sub *SubSdk) Auth() error {
	preAuthData, err := sub.preAuth()
	if err != nil {
		return err
	}

	connector, _ := preAuthData["connector"]
	if err := sub.connect(connector); err != nil {
		return err
	}

	if cmd, ok := CmdTable["auth"]; !ok {
		return errors.New("auth cmd not found")
	} else {
		cmdData, err := sub.prepareAuthCmd(preAuthData)
		if err != nil {
			return err
		}
		if err := sub.sendMessage([]string{cmd, string(cmdData)}); err != nil {
			return err
		} else {
			resp := make([]byte, 64)
			if _, err := sub.conn.Read(resp); err != nil {
				return err
			}
		}
	}
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
