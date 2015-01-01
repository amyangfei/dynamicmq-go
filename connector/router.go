package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"io"
	"net"
	"strings"
)

type DecodedMsg struct {
	extra   uint8
	bodyLen uint16
	items   map[uint8]string
}

type PubMsgFunc struct {
	validate func(msg *DecodedMsg) error
	process  func(msg *DecodedMsg) error
}

var RouterCmdTable = map[uint8]PubMsgFunc{
	PubMsgCmdPush: PubMsgFunc{validate: validateMsg, process: processMsg},
}

func StartRouter(bind string) error {
	log.Info("start router listening: %s", bind)
	go routerListen(bind)
	return nil
}

func routerListen(bind string) {
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
		go handleRouteConn(conn, rc)
	}
}

func handleRouteConn(conn net.Conn, rc chan *bufio.Reader) {
	addr := conn.RemoteAddr().String()
	log.Debug("addr: %s routine start", addr)

	for {
		rd := dmq.NewBufioReader(rc, conn, Config.TCPRecvBufSize)
		msg := make([]byte, Config.TCPRecvBufSize)
		if rlen, err := rd.Read(msg); err == nil {
			dmq.RecycleBufioReader(rc, rd)
			// TODO: route message to subscribers here
			log.Debug("addr: %s receive: %s", addr,
				strings.Replace(
					strings.Replace(string(msg[:rlen]), "\r", " ", -1), "\n", " ", -1))
			processReadBuffer(conn, msg)
			/* for redis-cli test code
			sendMsg := make([]byte, 0)
			AddReplyBulk(&sendMsg, string(msg[:rlen]))
			for _, cli := range SubcliTable {
				cli.conn.Write(sendMsg)
			}
			*/
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

func processReadBuffer(conn net.Conn, msg []byte) error {
	var remaining uint16 = uint16(len(msg))
	for {
		if remaining == 0 {
			return nil
		}
		if remaining <= PubMsgHeaderSize {
			log.Error("router recv error msg, invalid msg header length")
			return errors.New("invalid msg header len")
		}

		start := uint16(len(msg)) - remaining
		var cmd uint8 = msg[0]
		var bodyLen uint16 = binary.BigEndian.Uint16(msg[start+PubMsgCmdSize:])
		if bodyLen > PubMsgMaxBodyLen {
			log.Error("invalid request, invalid body length: %d", bodyLen)
			return errors.New("invalid msg body len")
		}
		if remaining >= PubMsgHeaderSize+bodyLen {
			decMsg, err := binaryMsgDecode(msg[start:], bodyLen)
			remaining -= (PubMsgHeaderSize + bodyLen)
			if err != nil {
				log.Error("invalid request")
				continue
			}
			if processFunc, ok := RouterCmdTable[cmd]; ok {
				if err := processFunc.validate(decMsg); err != nil {
					processFunc.process(decMsg)
				} else {
					// TODO: error handling
				}
			}
		}
	}
}

func binaryMsgDecode(msg []byte, bodyLen uint16) (*DecodedMsg, error) {
	var extra uint8 = msg[PubMsgCmdSize+PubMsgBodySize]
	decMsg := DecodedMsg{
		extra: extra, bodyLen: bodyLen, items: make(map[uint8]string, 0)}

	totalLen := PubMsgHeaderSize + bodyLen
	offset := PubMsgHeaderSize
	for offset < totalLen {
		if offset+PubMsgItemHeaderSize > totalLen {
			return nil, errors.New("invalid item header length")
		}
		itemLen := binary.BigEndian.Uint16(msg[offset+PubMsgItemIdSize:])
		if itemLen+PubMsgItemHeaderSize+offset > totalLen {
			return nil, errors.New("invalid item body length")
		}
		var itemId uint8 = msg[offset]
		decMsg.items[itemId] = string(
			msg[offset+PubMsgItemHeaderSize : offset+PubMsgItemHeaderSize+itemLen])
		offset += PubMsgItemHeaderSize + itemLen
	}

	return &decMsg, nil
}

func validateMsg(msg *DecodedMsg) error {
	switch msg.extra {
	case PubMsgExtraSendSingle, PubMsgExtraSendMulHead:
		if payload, ok := msg.items[PubMsgItemPayloadId]; !ok {
			return errors.New("msg payload item not found")
		} else if uint16(len(payload)) > PubMsgItemMaxPayload {
			return errors.New(
				fmt.Sprintf("msg payload too large: %d", len(payload)))
		}
	}

	if msgId, ok := msg.items[PubMsgItemMsgidId]; ok {
		if uint16(len(msgId)) != PubMsgItemMsgidSize {
			return errors.New("msgid item not found")
		}
	} else {
		return errors.New("msgid item not found")
	}

	return nil
}

func processMsg(msg *DecodedMsg) error {
	return nil
}
