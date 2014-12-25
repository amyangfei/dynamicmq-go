package main

import (
	dmq "../dynamicmq"
	"bufio"
	"io"
	"net"
	"strings"
)

func StartDispatcher(bind string) error {
	log.Info("start dispatcher listening: %s", bind)
	go dispatcherListen(bind)
	return nil
}

func dispatcherListen(bind string) {
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
			log.Error("SetReadBuffer(%d) error(%v)", Config.TCPRecvBufSize, err)
			conn.Close()
			continue
		}
		if err = conn.SetWriteBuffer(Config.TCPSendBufSize); err != nil {
			log.Error("SetWriteBuffer(%d) error(%v)", Config.TCPSendBufSize, err)
			conn.Close()
			continue
		}
		rc := recvTcpBufCache.Get()
		// one connection one routine
		go handleDispatchConn(conn, rc)
	}
}

func handleDispatchConn(conn net.Conn, rc chan *bufio.Reader) {
	addr := conn.RemoteAddr().String()
	log.Debug("addr: %s routine start", addr)

	for {
		rd := dmq.NewBufioReader(rc, conn, Config.TCPRecvBufSize)
		msg := make([]byte, Config.TCPRecvBufSize)
		if rlen, err := rd.Read(msg); err == nil {
			dmq.RecycleBufioReader(rc, rd)
			// TODO: dispatch message to subscribers here
			log.Debug("addr: %s receive: %s", addr,
				strings.Replace(
					strings.Replace(string(msg[:rlen]), "\r", " ", -1), "\n", " ", -1))
			sendMsg := make([]byte, 0)
			// AddReplyMultiBulk(&sendMsg, []string{string(msg[:rlen])})
			AddReplyBulk(&sendMsg, string(msg[:rlen]))
			for _, cli := range SubcliTable {
				cli.conn.Write(sendMsg)
			}
		} else {
			dmq.RecycleBufioReader(rc, rd)
			if err == io.EOF {
				log.Info("addr: %s close connection", addr)
				return
			}
			log.Error("addr: %s socket read error(%v)", addr, err)
			break
		}
	}

	// close the connection
	if err := conn.Close(); err != nil {
		log.Error("addr: %s conn.Close() error(%v)", addr, err)
	}
	log.Debug("addr: %s routine stop", addr)

}
