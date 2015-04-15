package main

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"gopkg.in/mgo.v2/bson"
	"io"
	"net"
	"time"
)

type MatchClient struct {
	id         bson.ObjectId // Matcher NodeId
	conn       net.Conn
	processBuf []byte
	processEnd int
}

type DecodedMsg struct {
	extra   uint8
	bodyLen uint16
	items   map[uint8]string
}

var (
	ProcessLater = errors.New("process later")
)

type HandleMsgFunc struct {
	validate func(msg *DecodedMsg) error
	process  func(msg *DecodedMsg, cli *MatchClient) error
}

var DispCmdTable = map[uint8]HandleMsgFunc{
	dmq.MDMsgCmdPushMsg:   HandleMsgFunc{validate: validatePushMsg, process: processPushMsg},
	dmq.MDMsgCmdHeartbeat: HandleMsgFunc{validate: validateHeartbeat, process: processHeartbeat},
}

func StartMatchTCP(bind string) error {
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
	var timeout time.Time = time.Now()
	timeout = timeout.Add(time.Second * time.Duration(MatchNodeDfltExpire))
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
			} else {
				log.Error("addr: %s read with error(%v)", addr, err)
				break
			}
		} else {
			err := processReadbuf(cli, cli.processBuf[:cli.processEnd+rlen])
			if err != nil && err != ProcessLater {
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
	var remaining uint16 = uint16(len(buf))
	for {
		if remaining == 0 {
			return nil
		}
		if remaining < dmq.MDMsgHeaderSize {
			return ProcessLater
		}
		start := uint16(len(buf)) - remaining
		var cmd uint8 = buf[start]
		var bodyLen uint16 = binary.BigEndian.Uint16(buf[start+dmq.MDMsgCmdSize:])
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
			if processFunc, ok := DispCmdTable[cmd]; ok {
				if err := processFunc.validate(decMsg); err != nil {
					log.Error("request valid error(%v)", err)
				} else {
					processFunc.process(decMsg, cli)
				}
			} else {
				log.Error("cmd: %d not support", cmd)
			}
		} else {
			return ProcessLater
		}
	}
	return nil
}

func binaryMsgDecode(msg []byte, bodyLen uint16) (*DecodedMsg, error) {
	var extra uint8 = msg[dmq.MDMsgCmdSize+dmq.MDMsgBodySize]
	decMsg := DecodedMsg{
		extra: extra, bodyLen: bodyLen, items: make(map[uint8]string, 0),
	}

	totalLen := dmq.MDMsgHeaderSize + bodyLen
	offset := dmq.MDMsgHeaderSize
	for offset < totalLen {
		if offset+dmq.MDMsgItemHeaderSize > totalLen {
			return nil, errors.New("invalid item header length")
		}
		itemLen := binary.BigEndian.Uint16(msg[offset+dmq.MDMsgItemIdSize:])
		if itemLen+dmq.MDMsgItemHeaderSize+offset > totalLen {
			return nil, errors.New("invalid item body length")
		}
		var itemId uint8 = msg[offset]
		decMsg.items[itemId] = string(
			msg[offset+dmq.MDMsgItemHeaderSize : offset+dmq.MDMsgItemHeaderSize+itemLen])
		offset += dmq.MDMsgItemHeaderSize + itemLen
	}

	return &decMsg, nil
}

func validatePushMsg(msg *DecodedMsg) error {
	if payload, ok := msg.items[dmq.MDMsgItemPayloadId]; !ok {
		return errors.New("msg payload item not found")
	} else if uint16(len(payload)) > dmq.MDMsgItemMaxPayload {
		return fmt.Errorf("msg payload too large %d", len(payload))
	}

	if msgId, ok := msg.items[dmq.MDMsgItemMsgidId]; !ok {
		return errors.New("msgid item not found")
	} else {
		if uint16(len(msgId)) != dmq.MDMsgItemMsgidSize {
			return errors.New("msgid item not found")
		}
	}

	if _, ok := msg.items[dmq.MDMsgItemSubListId]; !ok {
		return errors.New("subclient id list not found")
	}

	return nil
}

func processPushMsg(msg *DecodedMsg, cli *MatchClient) error {
	// Message must have been validated before processing
	msgId, _ := msg.items[dmq.MDMsgItemMsgidId]
	hexMsgId := hex.EncodeToString([]byte(msgId))
	msgPayload, _ := msg.items[dmq.MDMsgItemPayloadId]

	cliGroup := map[string][]byte{}
	subInfoList, _ := msg.items[dmq.MDMsgItemSubListId]
	step := dmq.SubClientIdSize + dmq.ConnectorNodeIdSize
	for i := 0; i+step <= len(subInfoList); i += step {
		subId := subInfoList[i : i+dmq.SubClientIdSize]
		connId := subInfoList[i+dmq.SubClientIdSize : i+step]
		cliGroup[connId] = append(cliGroup[connId], []byte(subId)...)
	}

	log.Debug("msgId: %s, msgPayload: %s", hexMsgId, msgPayload)
	for cid, subIds := range cliGroup {
		log.Debug("connid: %s, subids: %v", cid, hex.EncodeToString(subIds))

		msg := &BasicMsg{
			bodyLen: 0,
			extra:   dmq.DRMsgExtraNone,
			items: map[uint8]string{
				dmq.DRMsgItemMsgidId:   msgId,
				dmq.DRMsgItemPayloadId: msgPayload,
				dmq.DRMsgItemSubListId: string(subIds),
			},
		}

		if cid == RouterMgr.cid {
			msg.cmdType = dmq.DRMsgCmdPushMsg
			bmsg := binaryMsgEncode(msg)
			if err := RmSendMsg2Conn(RouterMgr, bmsg); err != nil {
				log.Error("send msg to connector error(%v)", err)
			}
		} else {
			// TODO: pack msg and redirect to other dispatcher
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
