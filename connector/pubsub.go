package main

import (
	"bufio"
	"net"
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
	// rb := newtcpBufCache()
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
		// one connection one routine
		go handleTCPConn(conn)
	}
}

func handleTCPConn(conn net.Conn) {
	addr := conn.RemoteAddr().String()
	log.Debug("handleTcpConn(%s) routine start", addr)

	rd := bufio.NewReaderSize(conn, Config.TCPRecvBufSize)
	for {
		buf := make([]byte, Config.TCPRecvBufSize)
		_, err := rd.Read(buf)
		if err != nil {
			log.Error("reading from %s error(%v)", addr, err)
			break
		}
		log.Debug("recv conn(%s) %s", addr, buf)
	}
	// close the connection
	if err := conn.Close(); err != nil {
		log.Error("%s conn.Close() error(%v)", addr, err)
	}
	log.Debug("handleTcpConn(%s) routine stop", addr)
}
