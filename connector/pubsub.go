package main

import (
	dmq "../dynamicmq"
	"bufio"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

type SubClient struct {
	id     string
	expire int64
	conn   net.Conn
	status int
}

var (
	IsPending int = 0x01
	IsAuthed  int = 0x02
	IsDisable int = 0x04
)

var (
	// hearbeat reply
	HeartbeatReply = []byte("+h\r\n")

	// command error reply
	WrongCmdReply = []byte("-command in wrong protocol\r\n")
)

var (
	ErrProtocol = errors.New("cmd format error")

	// default expire for a subscribe client 15 min
	DfltExpire int64 = 15 * 60

	PendingExpire int64 = 10
)

var (
	CmdTable map[string]func(c *SubClient, args []string) = map[string]func(c *SubClient, args []string){
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
			expire: time.Now().Unix() + DfltExpire,
			conn:   conn,
			status: IsPending,
		}
		rc := recvTcpBufCache.Get()
		// one connection one routine
		go handleTCPConn(subCli, rc)
	}
}

func setSubTimeout(cli *SubClient) error {
	var timeout time.Time = time.Now()
	if cli.status&IsPending > 0 {
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
		if args, err := parseCmd(rd); err == nil {
			// recycle buffer bufio.Reader
			dmq.RecycleBufioReader(rc, rd)
			if cmd, ok := CmdTable[args[0]]; ok {
				cmd(cli, args)
			} else {
				cli.conn.Write(WrongCmdReply)
				log.Warning("addr: %s unknown cmd: %s", addr, args[0])
			}
		} else {
			// recycle buffer bufio.Reader
			dmq.RecycleBufioReader(rc, rd)
			if err == io.EOF {
				log.Info("addr: %s close connection", addr)
				return
			} else {
				log.Error("addr: %s parseCmd() error(%v)", addr, err)
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
	cli.status &= ^IsPending
	log.Debug("subscribeHandle with argv: %s", args)
}

// TODO: use client read buffer for better processing
func parseCmd(rd *bufio.Reader) ([]string, error) {
	// get argument number
	argNum, err := parseCmdSize(rd, '*')
	if err != nil {
		if err != io.EOF {
			log.Error("cmd format error when finding '*' (%v)", err)
		}
		return nil, err
	}

	// TODO: argNum validation
	if argNum < 1 {
		log.Error("connector subscriber cmd arg number length error")
		return nil, errors.New("cmd argnum length error")
	}
	args := make([]string, 0, argNum)
	for i := 0; i < argNum; i++ {
		// get argument length
		cmdLen, err := parseCmdSize(rd, '$')
		if err != nil {
			log.Error("parseCmdSize(rd, '$') error(%v)", err)
			return nil, err
		}
		// get argument data
		d, err := parseCmdData(rd, cmdLen)
		if err != nil {
			log.Error("parseCmdData error(%v)", err)
			return nil, err
		}
		// append args
		args = append(args, strings.ToLower(string(d)))
	}
	return args, nil
}

// Parse request protocol cmd size.
func parseCmdSize(rd *bufio.Reader, prefix uint8) (int, error) {
	cs, err := rd.ReadBytes('\n')
	if err != nil {
		if err != io.EOF {
			log.Error("rd.ReadBytes('\\n') error(%v)", err)
		}
		return 0, err
	}
	csl := len(cs)
	// at least '(prefix)[0-9a-zA-Z]+\r\n', length >= 4
	if csl <= 3 || cs[0] != prefix || cs[csl-2] != '\r' {
		log.Error("cmd header length part(%s) error", cs)
		return 0, ErrProtocol
	}
	// skip '\r\n'
	cmdSize, err := strconv.Atoi(string(cs[1 : csl-2]))
	if err != nil {
		log.Error("parse cmd size error(%v)", err)
		return 0, ErrProtocol
	}
	return cmdSize, nil
}

// Get the sub request protocol cmd data excluding \r\n.
func parseCmdData(rd *bufio.Reader, cmdLen int) ([]byte, error) {
	d, err := rd.ReadBytes('\n')
	if err != nil {
		log.Error("rd.ReadBytes('\\n') error(%v)", err)
		return nil, err
	}
	dl := len(d)
	// check last \r\n
	if dl != cmdLen+2 || d[dl-2] != '\r' {
		log.Error("%v in wrong length or no \\r", d)
		return nil, ErrProtocol
	}
	return d[0:cmdLen], nil
}
