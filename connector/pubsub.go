package main

import (
	dmq "../dynamicmq"
	"bufio"
	"errors"
	"net"
	"strconv"
	"strings"
)

var (
	// hearbeat
	HeartbeatReply = []byte("+h\r\n")

	// command error reply
	WrongCmdReply = []byte("-c\r\n")
)

var (
	ErrProtocol = errors.New("cmd format error")
)

func StartTCP(bind string) error {
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
			log.Error("conn.SetReadBuffer(%d) error(%v)", Config.TCPRecvBufSize, err)
			conn.Close()
			continue
		}
		if err = conn.SetWriteBuffer(Config.TCPSendBufSize); err != nil {
			log.Error("conn.SetWriteBuffer(%d) error(%v)", Config.TCPSendBufSize, err)
			conn.Close()
			continue
		}
		rc := recvTcpBufCache.Get()
		// one connection one routine
		go handleTCPConn(conn, rc)
	}
}

func handleTCPConn(conn net.Conn, rc chan *bufio.Reader) {
	addr := conn.RemoteAddr().String()
	log.Debug("handleTcpConn(%s) routine start", addr)

	for {
		rd := dmq.NewBufioReader(rc, conn, Config.TCPRecvBufSize)
		if args, err := parseCmd(rd); err == nil {
			// recycle buffer bufio.Reader
			dmq.RecycleBufioReader(rc, rd)
			switch args[0] {
			case "subscribe":
				subscribeHandle(conn, args[1:])
				break
			default:
				conn.Write(WrongCmdReply)
				log.Warning("addr: %s unknown cmd: %s", addr, args[0])
			}
		} else {
			// recycle buffer bufio.Reader
			dmq.RecycleBufioReader(rc, rd)
			log.Error("addr: %s parseCmd() error(%v)", addr, err)
			break
		}
	}

	// close the connection
	if err := conn.Close(); err != nil {
		log.Error("addr: %s conn.Close() error(%v)", addr, err)
	}
	log.Debug("addr: %s handleTcpConn routine stop", addr)
}

func subscribeHandle(conn net.Conn, args []string) {
	log.Debug("subscribeHandle with args: %s", args)
}

func parseCmd(rd *bufio.Reader) ([]string, error) {
	// get argument number
	argNum, err := parseCmdSize(rd, '*')
	if err != nil {
		log.Error("cmd format error when finding '*' (%v)", err)
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
		log.Error("rd.ReadBytes('\\n') error(%v)", err)
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
