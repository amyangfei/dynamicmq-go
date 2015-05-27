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

// DispClient manages a connection from dispatcher
type DispClient struct {
	id          string // Dispatcher nodeid
	expire      int64  // in seconds
	conn        net.Conn
	status      int
	processBuf  []byte
	processEnd  int
	processWait int
}

// DecodedMsg stores message items
type DecodedMsg struct {
	extra   uint8
	bodyLen uint16
	items   map[uint8]string
}

type pubMsgFunc struct {
	validate func(msg *DecodedMsg, cli *DispClient) error
	process  func(msg *DecodedMsg, cli *DispClient) error
}

var routerCmdTable = map[uint8]pubMsgFunc{
	dmq.DRMsgCmdHandshake: pubMsgFunc{validate: validateHsMsg, process: processHsMsg},
	dmq.DRMsgCmdHeartbeat: pubMsgFunc{validate: validateHbMsg, process: processHbMsg},
	dmq.DRMsgCmdPushMsg:   pubMsgFunc{validate: validatePushMsg, process: processPushMsg},
}

var (
	processWaitMax = 2
)

func startRouter(bind string) error {
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
		if err = conn.SetReadBuffer(Config.TCPRecvBufSize * 2); err != nil {
			log.Error("SetReadBuffer(%d) error(%v)", Config.TCPRecvBufSize, err)
			conn.Close()
			continue
		}
		if err = conn.SetWriteBuffer(Config.TCPSendBufSize * 2); err != nil {
			log.Error("SetWriteBuffer(%d) error(%v)", Config.TCPSendBufSize, err)
			conn.Close()
			continue
		}
		rc := recvTcpBufCache.Get()
		dispCli := &DispClient{
			conn:        conn,
			processBuf:  make([]byte, Config.TCPRecvBufSize*2),
			processEnd:  0,
			expire:      time.Now().Unix() + int64(Config.DispKeepalive),
			processWait: 0,
		}
		// one connection one routine
		go handleRouteConn(dispCli, rc)
	}
}

func setDispTimeout(cli *DispClient) error {
	now := time.Now().Unix()
	if now > cli.expire {
		return errors.New("Dispatcher client already timeout")
	}
	return cli.conn.SetReadDeadline(
		time.Now().Add(time.Second * time.Duration(cli.expire-now)))
}

func handleRouteConn(cli *DispClient, rc chan *bufio.Reader) {
	addr := cli.conn.RemoteAddr().String()
	log.Debug("addr: %s routine start", addr)

	for {
		if err := setDispTimeout(cli); err != nil {
			log.Error("router connection set timeout error(%v)", err)
			break
		}
		rd := dmq.NewBufioReader(rc, cli.conn, Config.TCPRecvBufSize)
		rlen, err := rd.Read(cli.processBuf[cli.processEnd:])
		dmq.RecycleBufioReader(rc, rd)
		if err != nil {
			if err == io.EOF {
				log.Info("addr: %s close connection", addr)
				if err := registerWaiting(Config, EtcdCliPool); err != nil {
					log.Error("failed re-register etcd waiting error(%v)", err)
				}
				return
			}
			log.Error("addr: %s socket read error(%v)", addr, err)
			break
		} else if rlen == 0 {
			log.Error("router recv error msg, invalid msg size %d", rlen)
			break
		} else {
			// TODO: route message to subscribers here
			err := processReadBuffer(cli, cli.processBuf[:cli.processEnd+rlen])
			if err != nil {
				if err != errProcessLater {
					log.Error("process conn readbuf error(%v)", err)
					break
				}
			}
		}
	}

	// close the connection
	if err := cli.conn.Close(); err != nil {
		log.Error("addr: %s conn.Close() error(%v)", addr, err)
	}
	log.Debug("addr: %s routine stop", addr)
	if err := registerWaiting(Config, EtcdCliPool); err != nil {
		log.Error("failed to re-register to waiting list error(%v)", err)
	}
}

func processReadBuffer(cli *DispClient, msg []byte) error {
	remaining := uint16(len(msg))
	for {
		if remaining == 0 {
			cli.processEnd = 0
			return nil
		}
		if remaining <= dmq.DRMsgHeaderSize {
			if cli.processWait >= processWaitMax {
				log.Error("router recv error msg, invalid msg header length %d", remaining)
				return errors.New("invalid msg header len")
			}
			cli.processWait++
			cli.processEnd = int(remaining)
			copy(msg[:cli.processEnd], msg[len(msg)-int(remaining):])
			return errProcessLater
		}

		start := uint16(len(msg)) - remaining
		cmd := msg[start]
		bodyLen := binary.BigEndian.Uint16(msg[start+dmq.DRMsgCmdSize:])
		if bodyLen > dmq.DRMsgMaxBodyLen {
			log.Error("invalid request, invalid body length: %d", bodyLen)
			return errors.New("invalid msg body len")
		}
		if remaining < dmq.DRMsgHeaderSize+bodyLen {
			if cli.processWait >= processWaitMax {
				return errors.New("bad request body")
			}
			cli.processWait++
			cli.processEnd = int(remaining)
			copy(msg[:cli.processEnd], msg[len(msg)-int(remaining):])
			return errProcessLater
		}
		decMsg, err := binaryMsgDecode(msg[start:], bodyLen)
		remaining -= (dmq.DRMsgHeaderSize + bodyLen)
		if err != nil {
			log.Error("invalid request error(%v)", err)
			continue
		}
		if processFunc, ok := routerCmdTable[cmd]; ok {
			if err := processFunc.validate(decMsg, cli); err != nil {
				log.Error("invalid request error(%v)", err)
			} else {
				processFunc.process(decMsg, cli)
			}
		} else {
			log.Error("cmd %d not found", cmd)
		}

		// reset processWait
		if cli.processWait > 0 {
			cli.processWait = 0
		}
	}
}

func binaryMsgDecode(msg []byte, bodyLen uint16) (*DecodedMsg, error) {
	extra := msg[dmq.DRMsgCmdSize+dmq.DRMsgBodySize]
	decMsg := DecodedMsg{
		extra: extra, bodyLen: bodyLen, items: make(map[uint8]string, 0)}

	totalLen := dmq.DRMsgHeaderSize + bodyLen
	offset := dmq.DRMsgHeaderSize
	for offset < totalLen {
		if offset+dmq.DRMsgItemHeaderSize > totalLen {
			return nil, errors.New("invalid item header length")
		}
		itemLen := binary.BigEndian.Uint16(msg[offset+dmq.DRMsgItemIdSize:])
		if itemLen+dmq.DRMsgItemHeaderSize+offset > totalLen {
			return nil, errors.New("invalid item body length")
		}
		itemID := msg[offset]
		decMsg.items[itemID] = string(
			msg[offset+dmq.DRMsgItemHeaderSize : offset+dmq.DRMsgItemHeaderSize+itemLen])
		offset += dmq.DRMsgItemHeaderSize + itemLen
	}

	return &decMsg, nil
}

func validatePushMsg(msg *DecodedMsg, cli *DispClient) error {
	/*
		switch msg.extra {
		case dmq.DRMsgExtraSendSingle, dmq.DRMsgExtraSendMulHead:
			if payload, ok := msg.items[dmq.DRMsgItemPayloadId]; !ok {
				return errors.New("msg payload item not found")
			} else if uint16(len(payload)) > dmq.DRMsgItemMaxPayload {
				return errors.New(
					fmt.Sprintf("msg payload too large: %d", len(payload)))
			}
		}
	*/

	// We don't use local message cache currently, so each message packet from
	// dispatcher contains the message payload
	if payload, ok := msg.items[dmq.DRMsgItemPayloadId]; !ok {
		return errors.New("msg payload item not found")
	} else if uint16(len(payload)) > dmq.DRMsgItemMaxPayload {
		return fmt.Errorf("msg payload too large: %d", len(payload))
	}

	if msgID, ok := msg.items[dmq.DRMsgItemMsgidId]; !ok {
		return errors.New("msgid item not found")
	} else if uint16(len(msgID)) != dmq.DRMsgItemMsgidSize {
		return errors.New("msgid item not found")
	}

	if _, ok := msg.items[dmq.DRMsgItemSubListId]; !ok {
		return errors.New("sub list item not found")
	}

	return nil
}

func processPushMsg(msg *DecodedMsg, cli *DispClient) error {
	// message has been validated
	msgID, _ := msg.items[dmq.DRMsgItemMsgidId]
	payload, _ := msg.items[dmq.DRMsgItemPayloadId]
	subListStr, _ := msg.items[dmq.DRMsgItemSubListId]

	log.Debug("process msg id: %s extra: %d, bodyLen: %d",
		hex.EncodeToString([]byte(msgID)), msg.extra, msg.bodyLen)

	m := map[string]interface{}{
		"type":    PushMsg,
		"payload": payload,
	}
	bmsg, err := MsgPack(m)
	if err != nil {
		return err
	}

	for i := 0; i+dmq.SubClientIdSize <= len(subListStr); i += dmq.SubClientIdSize {
		subID := subListStr[i : i+dmq.SubClientIdSize]
		oid := bson.ObjectId(subID)
		if cli, ok := SubcliTable[oid]; !ok {
			log.Error("subclient with id %s not found", oid.Hex())
		} else {
			log.Debug("send msg to sub cli %s", cli.id.Hex())
			cli.SendMsg(bmsg)
		}
	}

	return nil
}

func validateHbMsg(msg *DecodedMsg, cli *DispClient) error {
	if ts, ok := msg.items[dmq.DRMsgItemTimestampId]; !ok {
		return errors.New("timestamp not in message")
	} else if uint16(len(ts)) != dmq.DRMsgItemTsSize {
		return errors.New("invalid timestamp size")
	}
	return nil
}

func processHbMsg(msg *DecodedMsg, cli *DispClient) error {
	ts := int64(binary.BigEndian.Uint64([]byte(msg.items[dmq.DRMsgItemTimestampId])))
	log.Debug("recv heartbeat from dispatcher %s", cli.id)
	expire := ts + int64(Config.DispKeepalive)
	if expire > cli.expire {
		cli.expire = expire
	}
	return nil
}

// validate handshake message
func validateHsMsg(msg *DecodedMsg, cli *DispClient) error {
	if nodeid, ok := msg.items[dmq.DRMsgItemDispidId]; !ok {
		return errors.New("dispatcher id not in message")
	} else if uint16(len(nodeid)) != dmq.DRMsgItemDispIdSize {
		return errors.New("invalid dispatcher id size")
	}
	return nil
}

func processHsMsg(msg *DecodedMsg, cli *DispClient) error {
	dispID := msg.items[dmq.DRMsgItemDispidId]
	log.Debug("recv handshake from dispatcher %s", dispID)
	cli.id = dispID
	return nil
}
