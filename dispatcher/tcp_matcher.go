package main

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"gopkg.in/mgo.v2/bson"
	"io"
	"net"
	"time"
)

// MatchClient manages connection of a matcher
type MatchClient struct {
	id         bson.ObjectId // Matcher NodeID
	conn       net.Conn
	processBuf []byte
	processEnd int
}

// DecodedMsg struct
type DecodedMsg struct {
	extra   uint8
	bodyLen uint16
	items   map[uint8]string
}

var (
	errProcessLater = errors.New("process later")
)

type handleMsgFunc struct {
	validate func(msg *DecodedMsg) error
	process  func(msg *DecodedMsg, cli *MatchClient) error
}

var dispCmdTable = map[uint8]handleMsgFunc{
	dmq.MDMsgCmdPushMsg:   handleMsgFunc{validate: validatePushMsg, process: processPushMsg},
	dmq.MDMsgCmdHeartbeat: handleMsgFunc{validate: validateHeartbeat, process: processHeartbeat},
}

func startMatchTCP(bind string) error {
	log.Info("start tcp listening: %s for matcher", bind)
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
	defer func() {
		log.Info("tcp addr: %s close", bind)
		if err := l.Close(); err != nil {
			log.Error("listener.Close() error(%v)", err)
		}
	}()

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
		matchCli := &MatchClient{
			conn:       conn,
			processBuf: make([]byte, Config.TCPRecvBufSize*2),
			processEnd: 0,
		}
		rc := recvTcpBufCache.Get()
		// one connection one routine
		go handleTCPConn(matchCli, rc)
	}
}

func setMatchNodeTimeout(cli *MatchClient) error {
	timeout := time.Now()
	timeout = timeout.Add(time.Second * time.Duration(matchNodeDfltExpire))
	return cli.conn.SetReadDeadline(timeout)
}

func handleTCPConn(cli *MatchClient, rc chan *bufio.Reader) {
	addr := cli.conn.RemoteAddr().String()
	log.Debug("MatcherClient handleTcpConn(%s) routine start", addr)

	for {
		if err := setMatchNodeTimeout(cli); err != nil {
			log.Error("MatchNodeClient set timeout error(%v)", err)
			break
		}

		rd := dmq.NewBufioReader(rc, cli.conn, Config.TCPRecvBufSize)
		rlen, err := rd.Read(cli.processBuf[cli.processEnd:])
		dmq.RecycleBufioReader(rc, rd)
		if err != nil {
			if err == io.EOF {
				log.Info("addr: %s close connection", addr)
				return
			}
			log.Error("addr: %s read with error(%v)", addr, err)
			break
		} else {
			err := processReadbuf(cli, cli.processBuf[:cli.processEnd+rlen])
			if err != nil && err != errProcessLater {
				log.Error("process MatcherClient conn readbuf error(%v)", err)
				break
			}
		}
	}

	// TODO: other clean work
	// close the connection
	if err := cli.conn.Close(); err != nil {
		log.Error("addr: %s conn.Close() error(%v)", addr, err)
	}
	log.Debug("addr: %s MatcherClient handleTcpConn routine stop", addr)

}

func processReadbuf(cli *MatchClient, buf []byte) error {
	remaining := uint16(len(buf))
	for {
		if remaining == 0 {
			cli.processEnd = 0
			return nil
		}
		if remaining < dmq.MDMsgHeaderSize {
			cli.processEnd = int(remaining)
			copy(buf[:cli.processEnd], buf[len(buf)-int(remaining):])
			return errProcessLater
		}
		start := uint16(len(buf)) - remaining
		cmd := buf[start]
		bodyLen := binary.BigEndian.Uint16(buf[start+dmq.MDMsgCmdSize:])
		if bodyLen > dmq.MDMsgMaxBodyLen {
			cli.processEnd = 0
			log.Error("invalid request, invalid body length: %d", bodyLen)
			return errors.New("invalid msg body len")
		}
		if remaining >= dmq.MDMsgHeaderSize+bodyLen {
			decMsg, err :=
				binaryMsgDecode(buf[start:], bodyLen)
			remaining -= (dmq.MDMsgHeaderSize + bodyLen)
			if err != nil {
				log.Error("invalid request error(%v)", err)
				continue
			}
			if processFunc, ok := dispCmdTable[cmd]; ok {
				if err := processFunc.validate(decMsg); err != nil {
					log.Error("request valid error(%v)", err)
				} else {
					processFunc.process(decMsg, cli)
				}
			} else {
				log.Error("cmd: %d not support", cmd)
			}
		} else {
			cli.processEnd = int(remaining)
			copy(buf[:cli.processEnd], buf[len(buf)-int(remaining):])
			return errProcessLater
		}
	}
}

func binaryMsgDecode(msg []byte, bodyLen uint16) (*DecodedMsg, error) {
	extra := msg[dmq.MDMsgCmdSize+dmq.MDMsgBodySize]
	decMsg := DecodedMsg{
		extra: extra, bodyLen: bodyLen, items: make(map[uint8]string, 0),
	}

	totalLen := dmq.MDMsgHeaderSize + bodyLen
	offset := dmq.MDMsgHeaderSize
	for offset < totalLen {
		if offset+dmq.MDMsgItemHeaderSize > totalLen {
			return nil, errors.New("invalid item header length")
		}
		itemLen := binary.BigEndian.Uint16(msg[offset+dmq.MDMsgItemIDSize:])
		if itemLen+dmq.MDMsgItemHeaderSize+offset > totalLen {
			return nil, errors.New("invalid item body length")
		}
		itemID := msg[offset]
		decMsg.items[itemID] = string(
			msg[offset+dmq.MDMsgItemHeaderSize : offset+dmq.MDMsgItemHeaderSize+itemLen])
		offset += dmq.MDMsgItemHeaderSize + itemLen
	}

	return &decMsg, nil
}

func validatePushMsg(msg *DecodedMsg) error {
	if payload, ok := msg.items[dmq.MDMsgItemPayloadID]; !ok {
		return errors.New("msg payload item not found")
	} else if uint16(len(payload)) > dmq.MDMsgItemMaxPayload {
		return fmt.Errorf("msg payload too large %d", len(payload))
	}

	if msgID, ok := msg.items[dmq.MDMsgItemMsgidID]; !ok {
		return errors.New("msgid item not found")
	} else if uint16(len(msgID)) != dmq.MDMsgItemMsgidSize {
		return errors.New("msgid item not found")
	}

	if _, ok := msg.items[dmq.MDMsgItemSubListID]; !ok {
		return errors.New("subclient id list not found")
	}

	return nil
}

func processPushMsg(msg *DecodedMsg, cli *MatchClient) error {
	// Message redirected from other dispatcher
	if msg.extra&dmq.DRMsgExtraRedirect > 0 {
		log.Debug("send %d redirected msg to connector",
			len(msg.items[dmq.MDMsgItemSubListID])/dmq.SubClientIDSize)
		rmsg := &BasicMsg{
			cmdType: dmq.DRMsgCmdPushMsg,
			bodyLen: 0,
			extra:   dmq.DRMsgExtraNone,
			items: map[uint8]string{
				dmq.DRMsgItemMsgidID:   msg.items[dmq.MDMsgItemMsgidID],
				dmq.DRMsgItemPayloadID: msg.items[dmq.MDMsgItemPayloadID],
				dmq.DRMsgItemSubListID: msg.items[dmq.MDMsgItemSubListID],
			},
		}
		bmsg := binaryMsgEncode(rmsg)
		rmSendMsg2Conn(RouterMgr, bmsg)
		return nil
	}

	// Message must have been validated before processing
	msgID, _ := msg.items[dmq.MDMsgItemMsgidID]
	msgPayload, _ := msg.items[dmq.MDMsgItemPayloadID]

	cliGroup := map[string][]byte{}
	subInfoList, _ := msg.items[dmq.MDMsgItemSubListID]
	step := dmq.SubClientIDSize + dmq.ConnectorNodeIDSize
	for i := 0; i+step <= len(subInfoList); i += step {
		subID := subInfoList[i : i+dmq.SubClientIDSize]
		connID := subInfoList[i+dmq.SubClientIDSize : i+step]
		cliGroup[connID] = append(cliGroup[connID], []byte(subID)...)
	}

	for cid, subIDs := range cliGroup {
		msg := &BasicMsg{
			cmdType: dmq.DRMsgCmdPushMsg,
			bodyLen: 0,
			extra:   dmq.DRMsgExtraNone,
			items: map[uint8]string{
				dmq.DRMsgItemMsgidID:   msgID,
				dmq.DRMsgItemPayloadID: msgPayload,
				dmq.DRMsgItemSubListID: string(subIDs),
			},
		}

		if cid == RouterMgr.cid {
			log.Debug("send %d direct msg to connector %s",
				len(subIDs)/dmq.SubClientIDSize, cid)
			bmsg := binaryMsgEncode(msg)
			rmSendMsg2Conn(RouterMgr, bmsg)
		} else {
			msg.extra |= dmq.DRMsgExtraRedirect
			bmsg := binaryMsgEncode(msg)
			dispMsgSender(cid, bmsg)
		}
	}

	return nil
}

func validateHeartbeat(msg *DecodedMsg) error {
	return nil
}

func processHeartbeat(msg *DecodedMsg, cli *MatchClient) error {
	log.Debug("recv heartbeat %v from match service datanode", msg)
	setMatchNodeTimeout(cli)
	cli.conn.Write([]byte("heartbeat received"))
	return nil
}
