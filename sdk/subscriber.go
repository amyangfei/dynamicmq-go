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
	"time"
)

type SubSdk struct {
	// cliId is a 12-byte hex string as identity used in external system, among
	// clients using sdk. The system doesn't care about how it is generated. It
	// is reserved for external interface.
	cliId       string
	token       string
	preAuthAddr string
	laddr       string
	conn        net.Conn
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

func NewSubSdk(cliId, preAuth string) (*SubSdk, error) {
	if !validSubClientId(cliId) {
		return nil, errors.New("invalid sub client id")
	}
	return &SubSdk{cliId: cliId, preAuthAddr: preAuth}, nil
}

func (sdk *SubSdk) SetLaddr(laddr string) {
	sdk.laddr = laddr
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
	var laddr *net.TCPAddr = nil
	if sub.laddr != "" {
		var err error
		if laddr, err = net.ResolveTCPAddr("tcp", sub.laddr); err != nil {
			return err
		}
	}
	conn, err := net.DialTCP("tcp", laddr, addr)
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
	return sub.preAuthAddr, nil
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

// attrnames is an array attribute name.
// attrvals is an array of json string represents attribute information
func (sub *SubSdk) Subscribe(attrnames, attrvals []string) error {
	if len(attrnames) != len(attrvals) {
		return fmt.Errorf("attrnames and attrvals not matching")
	}
	for _, attrval := range attrvals {
		if !isJSONString(attrval) {
			return fmt.Errorf("%v is not a validate json string", attrval)
		}
	}

	msg := make([]string, 0)
	msg = append(msg, CmdTable["subscribe"])
	for i := 0; i < len(attrnames); i++ {
		msg = append(msg, attrnames[i])
		msg = append(msg, attrvals[i])
	}

	if err := sub.sendMessage(msg); err != nil {
		sub.Close()
		return err
	}
	return nil
}

func (sub *SubSdk) Heartbeat() error {
	if cmd, ok := CmdTable["heartbeat"]; !ok {
		return errors.New("heartbeat cmd not found")
	} else {
		if err := sub.sendMessage([]string{cmd}); err != nil {
			sub.Close()
			return err
		}
		// get response data back in RecvMsgRoutine
	}
	return nil
}

func (sub *SubSdk) HeartbeatRoutine(interval int) {
	ticker := time.NewTicker(time.Second * time.Duration(interval))
	for {
		<-ticker.C
		sub.Heartbeat()
	}
}

func (sub *SubSdk) RecvMsgRoutine() {
	dataChan := make(chan []byte)
	errChan := make(chan error)
	go sub.recvMsg(dataChan, errChan)
	for {
		select {
		case data := <-dataChan:
			fmt.Printf("receive: %s", data)
		case err := <-errChan:
			fmt.Printf("receive error: %v", err)
			break
		}
	}
}

func (sub *SubSdk) recvMsg(dataChan chan []byte, errChan chan error) {
	for {
		buf := make([]byte, 2048)
		rlen, err := sub.conn.Read(buf)
		if err != nil {
			errChan <- err
			break
		} else {
			dataChan <- buf[:rlen]
		}
	}
}

func (sub *SubSdk) Close() error {
	return sub.conn.Close()
}

func isJSONString(s string) bool {
	var js map[string]interface{}
	return json.Unmarshal([]byte(s), &js) == nil
}
