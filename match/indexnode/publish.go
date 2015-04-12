package main

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"gopkg.in/mgo.v2/bson"
	"io"
	"net"
	"time"
)

var (
	DfltExpire int64 = 3 * 60
)

type PubClient struct {
	id         bson.ObjectId // used as client identity in internal system
	token      bson.ObjectId // used as client identity in external system
	expire     int64
	conn       net.Conn
	processBuf []byte
	processEnd int
}

type DecodedMsg struct {
	extra   uint8
	bodyLen uint16
	items   map[uint8]string
}

// support range attribute only
type PubMsgAttr struct {
	name string
	val  float64
}

type PubMsg struct {
	payload string
	attrs   []*PubMsgAttr
}

type HandleMsgFunc struct {
	validate func(msg *DecodedMsg) error
	process  func(msg *DecodedMsg) error
}

var PubCmdTable = map[uint8]HandleMsgFunc{
	dmq.PMMsgCmdPushMsg: HandleMsgFunc{validate: validatePushMsg, process: processPushMsg},
}

func StartPubTCP(bind string) {
	log.Info("start tcp listening: %s", bind)
	go pubTCPListen(bind)
}

func pubTCPListen(bind string) {
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
		pubCli := &PubClient{
			id:         bson.NewObjectId(),
			expire:     time.Now().Unix() + DfltExpire,
			conn:       conn,
			processBuf: make([]byte, Config.TCPRecvBufSize*2),
			processEnd: 0,
		}
		rc := recvTcpBufCache.Get()
		go handleTCPConn(pubCli, rc)
	}
}

func setPubTimeout(cli *PubClient) error {
	var timeout time.Time = time.Now()
	timeout = timeout.Add(time.Second * time.Duration(DfltExpire))
	return cli.conn.SetReadDeadline(timeout)
}

func handleTCPConn(cli *PubClient, rc chan *bufio.Reader) {
	addr := cli.conn.RemoteAddr().String()
	log.Debug("handleTcpConn(%s) routine start", addr)

	for {
		if err := setPubTimeout(cli); err != nil {
			log.Error("PubClient set timeout error(%v)", err)
			break
		}
		rd := dmq.NewBufioReader(rc, cli.conn, Config.TCPRecvBufSize)
		rlen, err := rd.Read(cli.processBuf[cli.processEnd:])
		dmq.RecycleBufioReader(rc, rd)
		if err != nil {
			if err == io.EOF {
				log.Info("addr: %s close connection", addr)
				// TODO: clean work for pub client
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

	// close the connection
	if err := cli.conn.Close(); err != nil {
		log.Error("addr: %s conn.Close() error(%v)", addr, err)
	}

	log.Debug("addr: %s handleTcpConn routine stop", addr)
}

func processReadbuf(cli *PubClient, buf []byte) error {
	var remaining uint16 = uint16(len(buf))
	for {
		if remaining == 0 {
			return nil
		}
		if remaining < dmq.PMMsgHeaderSize {
			return ProcessLater
		}
		start := uint16(len(buf)) - remaining
		var cmd uint8 = buf[start]
		var bodyLen uint16 = binary.BigEndian.Uint16(buf[start+dmq.PMMsgCmdSize:])
		if bodyLen > dmq.PMMsgMaxBodyLen {
			cli.processEnd = 0
			log.Error("invalid request, invalid body length: %d", bodyLen)
			return fmt.Errorf("invalid msg body len")
		}
		if remaining >= dmq.PMMsgHeaderSize+bodyLen {
			decMsg, err := binaryMsgDecode(buf[start:], bodyLen)
			remaining -= (dmq.PMMsgHeaderSize + bodyLen)
			if err != nil {
				log.Error("invalid request error(%v)", err)
				continue
			}
			if processFunc, ok := PubCmdTable[cmd]; ok {
				if err := processFunc.validate(decMsg); err != nil {
					log.Error("cmd %d request valid error(%v)", cmd, err)
				} else {
					if perr := processFunc.process(decMsg); perr != nil {
						log.Error("process error: %v", perr)
					}
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
	var extra uint8 = msg[dmq.PMMsgCmdSize+dmq.PMMsgBodySize]
	decMsg := DecodedMsg{
		extra: extra, bodyLen: bodyLen, items: make(map[uint8]string, 0),
	}

	totalLen := dmq.PMMsgHeaderSize + bodyLen
	offset := dmq.PMMsgHeaderSize
	for offset < totalLen {
		if offset+dmq.PMMsgItemHeaderSize > totalLen {
			return nil, fmt.Errorf("invalid item header length")
		}
		itemLen := binary.BigEndian.Uint16(msg[offset+dmq.PMMsgItemIdSize:])
		if itemLen+dmq.PMMsgItemHeaderSize+offset > totalLen {
			return nil, fmt.Errorf("invalid item body length")
		}
		var itemId uint8 = msg[offset]
		decMsg.items[itemId] = string(
			msg[offset+dmq.PMMsgItemHeaderSize : offset+dmq.PMMsgItemHeaderSize+itemLen])
		offset += dmq.PMMsgItemHeaderSize + itemLen
	}

	return &decMsg, nil
}

func validatePushMsg(msg *DecodedMsg) error {
	if _, ok := msg.items[dmq.PMMsgItemPayloadId]; !ok {
		return fmt.Errorf("msg payload item not found")
	}

	if _, ok := msg.items[dmq.PMMsgItemAttributeId]; !ok {
		return fmt.Errorf("msg attribute item not found")
	}

	return nil
}

func createPubMsg(attrs map[string]interface{}, payload string) *PubMsg {
	pmsg := &PubMsg{
		payload: payload,
		attrs:   make([]*PubMsgAttr, 0),
	}
	for name, attr := range attrs {
		// only handling attribute defined in IndexBase
		if ab, ok := IdxBase.attrbases[name]; ok {
			// support range attribute only curently
			if int(ab.use) == dmq.AttrUseField["range"] {
				if f, ok := attr.(float64); ok {
					pmsg.attrs = append(
						pmsg.attrs, &PubMsgAttr{name: name, val: f})
				}
			}
		}
	}
	return pmsg
}

func processPushMsg(msg *DecodedMsg) error {
	// Message must have been validated before processing
	log.Debug("recv %v from publisher", msg)

	payload := msg.items[dmq.PMMsgItemPayloadId]
	attrs := msg.items[dmq.PMMsgItemAttributeId]
	parsedAttrs := make(map[string]interface{})
	if err := json.Unmarshal([]byte(attrs), &parsedAttrs); err != nil {
		return err
	}
	pubmsg := createPubMsg(parsedAttrs, payload)

	bestAttrIdx := filterBestAttrIndex(pubmsg)
	// TODO: send candinate subscribers to datanode
	log.Debug("best attr idx %v", bestAttrIdx)

	return nil
}

func filterBestAttrIndex(pubmsg *PubMsg) *AttrIndex {
	minMatchCnt := len(ClisInfo)
	bestAttrComb := ""

	attrNum := len(pubmsg.attrs)
	for i := 0; i < attrNum-1; i++ {
		for j := i + 1; j < attrNum; j++ {
			cname := AttrNameCombine(
				pubmsg.attrs[i].name, pubmsg.attrs[j].name)
			if attrIdx, ok := AttrIdxesMap[cname]; !ok {
				log.Warning("combine-name %s not found in AttrIdxesMap", cname)
			} else {
				x, y := i, j
				if !AttrNameLess(pubmsg.attrs[i].name, pubmsg.attrs[j].name) {
					x, y = j, i
				}
				cnt := attrIdx.tree.QueryCount(
					pubmsg.attrs[x].val, pubmsg.attrs[y].val)
				if cnt < minMatchCnt {
					minMatchCnt = cnt
					bestAttrComb = cname
				}
				log.Debug("c-name %s search count %d", cname, cnt)
			}
		}
	}

	return AttrIdxesMap[bestAttrComb]
}
