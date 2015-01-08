package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"gopkg.in/mgo.v2/bson"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type SubClient struct {
	id         string // used as client identity in internal system
	token      string // used as client identity in external system
	expire     int64
	conn       net.Conn
	status     int
	processBuf []byte
	processEnd int
}

var (
	SubcliIsPending int = 0x01
	SubcliIsAuthed  int = 0x02
	SubcliIsDisable int = 0x04
)

var (
	// hearbeat reply
	HeartbeatReply = []byte("+h" + dmq.Crlf)

	// auth success reply
	AuthSuccessReply = []byte("+authsuccess" + dmq.Crlf)

	// command error reply
	WrongCmdReply = []byte("-command in wrong protocol" + dmq.Crlf)
)

var (
	ErrProtocol = errors.New("cmd format error")

	ProcessLater = errors.New("process Later")

	// default expire for a subscribe client 15 min
	DfltExpire int64 = 15 * 60

	PendingExpire int64 = 10
)

var (
	CmdTable = map[string]func(c *SubClient, args []string) error{
		// TODO: add register command
		"auth": processAuth,
		"sub":  processSubscribe,
		"hb":   processHeartbeat,
	}
)

func StartSubTCP(bind string) error {
	log.Info("start tcp listening: %s", bind)
	go tcpListen(bind)
	return nil
}

func tcpListen(bind string) {
	addr, err := net.ResolveTCPAddr("tcp", bind)
	if err != nil {
		log.Error("net.ResolveTCPAddr(%s) error", bind)
		panic(err)
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		log.Error("net.ListenTCP(%s) error", bind, err)
		panic(err)
	}
	// free the listener
	defer func() {
		log.Info("tcp addr: %s close", bind)
		if err := l.Close(); err != nil {
			log.Error("listener.Close() error(%v)", err)
		}
	}()

	// init reader buffer instance
	recvTcpBufCache := dmq.NewTcpBufCache(Config.TCPBufInsNum, Config.TCPBufioNum)
	for {
		conn, err := l.AcceptTCP()
		if err != nil {
			log.Error("listener.AcceptTCP() error(%v)", err)
			continue
		}
		if err = conn.SetKeepAlive(true); err != nil {
			log.Error("conn.SetKeepAlive() error(%v)", err)
			conn.Close()
			continue
		}
		if err = conn.SetReadBuffer(Config.TCPRecvBufSize * 2); err != nil {
			log.Error("conn.SetReadBuffer(%d) error(%v)",
				Config.TCPRecvBufSize, err)
			conn.Close()
			continue
		}
		if err = conn.SetWriteBuffer(Config.TCPSendBufSize * 2); err != nil {
			log.Error("conn.SetWriteBuffer(%d) error(%v)",
				Config.TCPSendBufSize, err)
			conn.Close()
			continue
		}
		subCli := &SubClient{
			id:         bson.NewObjectId().Hex(),
			expire:     time.Now().Unix() + DfltExpire,
			conn:       conn,
			status:     SubcliIsPending,
			processBuf: make([]byte, Config.TCPRecvBufSize*2),
			processEnd: 0,
		}
		SubcliTable[subCli.id] = subCli
		rc := recvTcpBufCache.Get()
		// one connection one routine
		go handleTCPConn(subCli, rc)
	}
}

func setSubTimeout(cli *SubClient) error {
	var timeout time.Time = time.Now()
	if cli.status&SubcliIsPending > 0 {
		timeout = timeout.Add(time.Second * time.Duration(PendingExpire))
	} else {
		timeout = timeout.Add(time.Second * time.Duration(DfltExpire))
	}
	return cli.conn.SetReadDeadline(timeout)
}

func handleTCPConn(cli *SubClient, rc chan *bufio.Reader) {
	addr := cli.conn.RemoteAddr().String()
	log.Debug("handleTcpConn(%s) routine start", addr)

	for {
		if err := setSubTimeout(cli); err != nil {
			log.Error("SubClient set timeout error(%v)", err)
			break
		}
		rd := dmq.NewBufioReader(rc, cli.conn, Config.TCPRecvBufSize)
		rlen, err := rd.Read(cli.processBuf[cli.processEnd:])
		dmq.RecycleBufioReader(rc, rd)
		if err != nil {
			if err == io.EOF {
				log.Info("addr: %s close connection", addr)
				return
			} else {
				log.Error("addr: %s read with error(%v)", addr, err)
				break
			}
		} else {
			err := processReadbuf(cli, cli.processBuf[:cli.processEnd+rlen])
			if err != nil && err != ProcessLater {
				log.Error("process conn readbuf error(%v)", err)
				break
			}
		}
	}

	// TODO: other clean work
	// close the connection
	if err := cli.conn.Close(); err != nil {
		log.Error("addr: %s conn.Close() error(%v)", addr, err)
	}
	log.Debug("addr: %s handleTcpConn routine stop", addr)
}

func processAuth(cli *SubClient, args []string) error {
	commonErr := errors.New("processAuth error")
	if len(args) < 2 {
		log.Error("error auth cmd length: %d", len(args))
		return commonErr
	}
	authData := map[string]string{}
	if err := json.Unmarshal([]byte(args[1]), &authData); err != nil {
		log.Error("unmarshal auth info error(%v)", err)
		return commonErr
	}

	client_id, ok := authData["client_id"]
	if !ok {
		log.Error("client_id not found")
		return commonErr
	}
	timestamp, ok := authData["timestamp"]
	if !ok {
		log.Error("timestamp not found")
		return commonErr
	}
	token, ok := authData["token"]
	if !ok {
		log.Error("token not found")
		return commonErr
	}

	authUrl := fmt.Sprintf("http://%s/conn/sub/auth", Config.AuthSrvAddr)
	postData := url.Values{}
	postData.Add("client_id", client_id)
	postData.Add("timestamp", timestamp)
	postData.Add("token", token)

	client := &http.Client{}
	r, _ := http.NewRequest("POST", authUrl, bytes.NewBufferString(postData.Encode()))
	r.Header.Add("Content-Type", "application/x-www-form-urlencoded")
	r.Header.Add("Content-Length", strconv.Itoa(len(postData.Encode())))

	resp, err := client.Do(r)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	parsed := map[string]string{}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return err
	}
	if status, ok := parsed["status"]; !ok {
		return fmt.Errorf("status field not in auth response")
	} else if status != "ok" {
		log.Info("client %s auth failed", cli.id)
		return fmt.Errorf("auth failed")
	}
	cli.status &= ^SubcliIsPending
	cli.status |= SubcliIsAuthed
	log.Info("sub client %s auth successfully", cli.id)
	cli.conn.Write(AuthSuccessReply)
	return nil
}

func processHeartbeat(cli *SubClient, args []string) error {
	log.Debug("receive addr: %s heartbeat", cli.conn.RemoteAddr())
	cli.conn.Write(HeartbeatReply)
	return nil
}

func processSubscribe(cli *SubClient, args []string) error {
	// FIXME: should set this status in register process
	cli.status &= ^SubcliIsPending
	log.Debug("subscribeHandle with argv: %s", args)
	return nil
}

func processReadbuf(cli *SubClient, msg []byte) error {
	pos := 0
	for pos < len(msg) {
		if args, err := parseCmd(msg, &pos); err != nil {
			if err == ProcessLater {
				cli.processEnd = len(msg) - pos
				copy(msg[:cli.processEnd], msg[pos:])
				return ProcessLater
			} else {
				log.Error("%v", err)
				return ErrProtocol
			}
		} else {
			if cmd, ok := CmdTable[args[0]]; !ok {
				cli.conn.Write(WrongCmdReply)
				log.Warning("client: %s sent unknown cmd: %s", cli.id, args[0])
				return ErrProtocol
			} else {
				if err := cmd(cli, args); err != nil {
					return err
				}
			}
		}
	}
	cli.processEnd = 0
	return nil
}

// TODO: use client read buffer for better processing
func parseCmd(msg []byte, pos *int) ([]string, error) {
	packLen, err := parseSize(msg, pos, '#')
	if err != nil {
		// including ProcessLater error
		return nil, err
	}
	if packLen > len(msg)-*pos {
		// pos back of '#', packLen, '\r\n'
		*pos -= (3 + len(fmt.Sprintf("%d", packLen)))
		return nil, ProcessLater
	}

	argNum, err := parseSize(msg, pos, '*')
	if err != nil {
		return nil, err
	}
	if argNum < 1 {
		return nil, errors.New("cmd argnum length less than 1")
	}
	args := make([]string, 0, argNum)
	for i := 0; i < argNum; i++ {
		dataLen, err := parseSize(msg, pos, '$')
		if err != nil {
			return nil, fmt.Errorf("parseSize error (%v)", err)
		}
		d, err := parseData(msg, pos, dataLen)
		if err != nil {
			return nil, fmt.Errorf("parseData error (%v)", err)
		}
		args = append(args, strings.ToLower(string(d)))
	}
	return args, nil
}

func parseSize(msg []byte, pos *int, prefix uint8) (int, error) {
	// msg may be in different bufio buffer, process later
	// at most #[0-9]{4}\r\n, length=7
	if prefix == '#' && len(msg)-*pos < 7 {
		return 0, ProcessLater
	}
	if i := bytes.IndexByte(msg[*pos:], '\n'); i < 0 {
		return 0, errors.New("\\n not found")
	} else {
		// at least '(prefix)[0-9a-zA-Z]+\r\n', length >= 4, i >= 3
		if i <= 2 || msg[*pos] != prefix || msg[*pos+i-1] != '\r' {
			return 0, errors.New("cmd header length part error")
		}
		cmdSize, err := strconv.Atoi(string(msg[*pos+1 : *pos+i-1]))
		// skip '\r\n'
		*pos += i + 1
		if err != nil {
			return 0, fmt.Errorf("parse cmd size error(%v)", err)
		}
		return cmdSize, nil
	}
}

func parseData(msg []byte, pos *int, dataLen int) ([]byte, error) {
	if i := bytes.IndexByte(msg[*pos:], '\n'); i < 0 {
		return nil, errors.New("\\n not found in sub protocol")
	} else {
		// check last \r\n
		if i != dataLen+1 || msg[*pos+i-1] != '\r' {
			return nil, errors.New("data in wrong length or no \\r")
		} else {
			// skip data and '\r\n'
			*pos += dataLen + 2
			return msg[*pos-dataLen-2 : *pos-2], nil
		}
	}
}
