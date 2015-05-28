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

// SubSdk struct
type SubSdk struct {
	// cliID is a 12-byte hex string as identity used in external system, among
	// clients using sdk. The system doesn't care about how it is generated. It
	// is reserved for external interface.
	cliID       string
	token       string
	preAuthAddr string
	laddr       string
	conn        net.Conn
}

var (
	// mapping from command name to command content should be sent
	cmdTable = map[string]string{
		"auth":      "auth",
		"subscribe": "sub",
		"heartbeat": "hb",
	}
)

func validSubClientID(cliID string) bool {
	r, _ := regexp.Compile("^[0-9a-fA-F]{24}$")
	return r.MatchString(cliID)
}

// NewSubSdk creates a new SubSdk object
func NewSubSdk(cliID, preAuth string) (*SubSdk, error) {
	if !validSubClientID(cliID) {
		return nil, errors.New("invalid sub client id")
	}
	return &SubSdk{cliID: cliID, preAuthAddr: preAuth}, nil
}

// SetLaddr sets the sdk binding TCP address
func (sdk *SubSdk) SetLaddr(laddr string) {
	sdk.laddr = laddr
}

func (sdk *SubSdk) sendMessage(msgs []string) error {
	var sendMsg []byte
	sendMsg = append(sendMsg, fmt.Sprintf("*%d%s", len(msgs), dmq.Crlf)...)
	for _, msg := range msgs {
		sendMsg = append(sendMsg, fmt.Sprintf("$%d%s", len(msg), dmq.Crlf)...)
		sendMsg = append(sendMsg, (msg + dmq.Crlf)...)
	}
	// add '#len\r\n'
	sendMsg = append(
		[]byte(fmt.Sprintf("#%d%s", len(sendMsg), dmq.Crlf)), sendMsg...)
	sent, err := sdk.conn.Write(sendMsg)
	if sent != len(sendMsg) {
		return fmt.Errorf("send incomplete message should: %d sent: %d",
			len(sendMsg), sent)
	}
	return err
}

func (sdk *SubSdk) connect(dst string) error {
	addr, err := net.ResolveTCPAddr("tcp", dst)
	if err != nil {
		return err
	}
	var laddr *net.TCPAddr
	if sdk.laddr != "" {
		var err error
		if laddr, err = net.ResolveTCPAddr("tcp", sdk.laddr); err != nil {
			return err
		}
	}
	conn, err := net.DialTCP("tcp", laddr, addr)
	if err != nil {
		return err
	}
	if err := conn.SetKeepAlive(true); err != nil {
		sdk.Close()
		return err
	}
	sdk.conn = conn
	return nil
}

func (sdk *SubSdk) getPreAuthHost() (string, error) {
	// TODO: get preauth addr from global config service like etcd
	return sdk.preAuthAddr, nil
}

func (sdk *SubSdk) preAuth() (map[string]string, error) {
	preAuthHost, err := sdk.getPreAuthHost()
	if err != nil {
		return nil, err
	}

	preAuthURL := fmt.Sprintf("http://%s/sdk/sub/preauth?client_id=%s", preAuthHost, sdk.cliID)
	resp, err := http.Get(preAuthURL)
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

func (sdk *SubSdk) prepareAuthCmd(data map[string]string) ([]byte, error) {
	timestamp, _ := data["timestamp"]
	token, _ := data["token"]

	cmdData := map[string]string{
		"client_id": sdk.cliID,
		"timestamp": timestamp,
		"token":     token,
	}
	return json.Marshal(cmdData)
}

// Auth does the following task in sequence:
// 1) preauth to authsrv
// 2) connect to a connector
// 3) send auth cmd to connector
func (sdk *SubSdk) Auth() error {
	preAuthData, err := sdk.preAuth()
	if err != nil {
		return err
	}

	connector, _ := preAuthData["connector"]
	if err := sdk.connect(connector); err != nil {
		return err
	}

	cmd, ok := cmdTable["auth"]
	if !ok {
		return errors.New("auth cmd not found")
	}
	cmdData, err := sdk.prepareAuthCmd(preAuthData)
	if err != nil {
		return err
	}
	if err := sdk.sendMessage([]string{cmd, string(cmdData)}); err != nil {
		return err
	}
	resp := make([]byte, 64)
	if _, err := sdk.conn.Read(resp); err != nil {
		return err
	}
	return nil
}

// Subscribe is used when sdk wants to create new subsrciption or update
// existing subscription.
// attrnames is an array attribute name.
// attrvals is an array of json string represents attribute information
func (sdk *SubSdk) Subscribe(attrnames, attrvals []string) error {
	if len(attrnames) != len(attrvals) {
		return fmt.Errorf("attrnames and attrvals not matching")
	}
	for _, attrval := range attrvals {
		if !isJSONString(attrval) {
			return fmt.Errorf("%v is not a validate json string", attrval)
		}
	}

	var msg []string
	msg = append(msg, cmdTable["subscribe"])
	for i := 0; i < len(attrnames); i++ {
		msg = append(msg, attrnames[i])
		msg = append(msg, attrvals[i])
	}

	if err := sdk.sendMessage(msg); err != nil {
		sdk.Close()
		return err
	}
	return nil
}

func (sdk *SubSdk) heartbeat() error {
	cmd, ok := cmdTable["heartbeat"]
	if !ok {
		return errors.New("heartbeat cmd not found")
	}
	if err := sdk.sendMessage([]string{cmd}); err != nil {
		sdk.Close()
		return err
	}
	// get response data back in RecvMsgRoutine

	return nil
}

// HeartbeatRoutine is a standalone goroutine for heartbeat to connector
func (sdk *SubSdk) HeartbeatRoutine(interval int) {
	ticker := time.NewTicker(time.Second * time.Duration(interval))
	for {
		<-ticker.C
		sdk.heartbeat()
	}
}

// RecvMsgRoutine is a standalone goroutine for message receiving
func (sdk *SubSdk) RecvMsgRoutine() {
	dataChan := make(chan []byte)
	errChan := make(chan error)
	go sdk.recvMsg(dataChan, errChan)
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

func (sdk *SubSdk) recvMsg(dataChan chan []byte, errChan chan error) {
	for {
		buf := make([]byte, 2048)
		rlen, err := sdk.conn.Read(buf)
		if err != nil {
			errChan <- err
			break
		} else {
			dataChan <- buf[:rlen]
		}
	}
}

// Close should be called when we want to destory a subsdk
func (sdk *SubSdk) Close() error {
	if sdk.conn != nil {
		return sdk.conn.Close()
	}
	return nil
}

func isJSONString(s string) bool {
	var js map[string]interface{}
	return json.Unmarshal([]byte(s), &js) == nil
}
