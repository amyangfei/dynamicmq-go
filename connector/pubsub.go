package main

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"gopkg.in/mgo.v2/bson"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

type SubClient struct {
	id     string // used as client identity in internal system
	token  string // used as client identity in external system
	expire int64
	conn   net.Conn
	status int
}

var (
	SubcliIsPending int = 0x01
	SubcliIsAuthed  int = 0x02
	SubcliIsDisable int = 0x04
)

var (
	// hearbeat reply
	HeartbeatReply = []byte("+h" + dmq.Crlf)

	// command error reply
	WrongCmdReply = []byte("-command in wrong protocol" + dmq.Crlf)
)

var (
	ErrProtocol = errors.New("cmd format error")

	// default expire for a subscribe client 15 min
	DfltExpire int64 = 15 * 60

	PendingExpire int64 = 10
)

var (
	CmdTable = map[string]func(c *SubClient, args []string){
		// TODO: add register command
		"sub":       processSubscribe,
		"subscribe": processSubscribe,
		"hb":        processHeartbeat,
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
		if err = conn.SetReadBuffer(Config.TCPRecvBufSize); err != nil {
			log.Error("conn.SetReadBuffer(%d) error(%v)",
				Config.TCPRecvBufSize, err)
			conn.Close()
			continue
		}
		if err = conn.SetWriteBuffer(Config.TCPSendBufSize); err != nil {
			log.Error("conn.SetWriteBuffer(%d) error(%v)",
				Config.TCPSendBufSize, err)
			conn.Close()
			continue
		}
		subCli := &SubClient{
			id:     bson.NewObjectId().Hex(),
			expire: time.Now().Unix() + DfltExpire,
			conn:   conn,
			status: SubcliIsPending,
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
		msg := make([]byte, Config.TCPRecvBufSize)
		rlen, err := rd.Read(msg)
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
			if err := processReadbuf(cli, msg[:rlen]); err != nil {
				log.Error("process conn readbuf error(%v)", err)
				break
			}
		}
	}

	// close the connection
	if err := cli.conn.Close(); err != nil {
		log.Error("addr: %s conn.Close() error(%v)", addr, err)
	}
	log.Debug("addr: %s handleTcpConn routine stop", addr)
}

func processHeartbeat(cli *SubClient, args []string) {
	log.Debug("receive addr: %s heartbeat", cli.conn.RemoteAddr())
	cli.conn.Write(HeartbeatReply)
}

func processSubscribe(cli *SubClient, args []string) {
	// FIXME: should set this status in register process
	cli.status &= ^SubcliIsPending
	log.Debug("subscribeHandle with argv: %s", args)
}

func processReadbuf(cli *SubClient, msg []byte) error {
	pos := 0
	for pos < len(msg) {
		if args, err := parseCmd(msg, &pos); err != nil {
			log.Error("%v", err)
			return ErrProtocol
		} else {
			if cmd, ok := CmdTable[args[0]]; !ok {
				cli.conn.Write(WrongCmdReply)
				log.Warning("client: %s sent unknown cmd: %s", cli.id, args[0])
				return ErrProtocol
			} else {
				cmd(cli, args)
			}
		}
	}
	return nil
}

// TODO: use client read buffer for better processing
func parseCmd(msg []byte, pos *int) ([]string, error) {
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
			return nil, errors.New(fmt.Sprintf("parseSize error (%v)", err))
		}
		d, err := parseData(msg, pos, dataLen)
		if err != nil {
			return nil, errors.New(fmt.Sprintf("parseData error (%v)", err))
		}
		args = append(args, strings.ToLower(string(d)))
	}
	return args, nil
}

func parseSize(msg []byte, pos *int, prefix uint8) (int, error) {
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
			return 0, errors.New(fmt.Sprintf("parse cmd size error(%v)", err))
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
