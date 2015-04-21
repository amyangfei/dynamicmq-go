package main

import (
	"bufio"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"gopkg.in/mgo.v2/bson"
	"io"
	"net"
	"time"
)

// TODO: uniform Attribute structure, also used in connector module.
type Attribute struct {
	name   string
	use    byte
	strval string
	low    float64
	high   float64
	extra  string
}

type PubAttr struct {
	name   string
	use    int
	strval string
	val    float64
}

type SubCliInfo struct {
	Cid     []byte                // subscribe client's Id
	CidHash []byte                // cid's hash in datanode
	ConnId  string                // id of connector the subclient connecting with
	Attrs   []*Attribute          // subscription attribute array
	AttrMap map[string]*Attribute // used for accelerating matching
}

type IdxNodeClient struct {
	expire     int64
	conn       net.Conn
	processBuf []byte
	processEnd int
}

var (
	DfltExpire int64 = 15 * 60

	ProcessLater = fmt.Errorf("process Later")
)

// basic data structure for binary message
type BasicMsg struct {
	cmdType uint8
	bodyLen uint16
	extra   uint8
	items   map[uint8]string
}

// Decoded binary message
type DecodedMsg struct {
	extra   uint8
	bodyLen uint16
	items   map[uint8]string
}

type HandleMsgFunc struct {
	validate func(msg *DecodedMsg) error
	process  func(msg *DecodedMsg, cli *IdxNodeClient) error
}

var PubCmdTable = map[uint8]HandleMsgFunc{
	dmq.IDMsgCmdPushMsg:      HandleMsgFunc{validate: validatePushMsg, process: processPushMsg},
	dmq.IDMsgCmdHeartbeatMsg: HandleMsgFunc{validate: validateHeartbeat, process: processHeartbeat},
}

func binaryMsgDecode(msg []byte, bodyLen uint16) (*DecodedMsg, error) {
	var extra uint8 = msg[dmq.IDMsgCmdSize+dmq.IDMsgBodySize]
	decMsg := DecodedMsg{
		extra: extra, bodyLen: bodyLen, items: make(map[uint8]string, 0),
	}

	totalLen := dmq.IDMsgHeaderSize + bodyLen
	offset := dmq.IDMsgHeaderSize
	for offset < totalLen {
		if offset+dmq.IDMsgItemHeaderSize > totalLen {
			return nil, fmt.Errorf("invalid item header length")
		}
		itemLen := binary.BigEndian.Uint16(msg[offset+dmq.IDMsgItemIdSize:])
		if itemLen+dmq.IDMsgItemHeaderSize+offset > totalLen {
			return nil, fmt.Errorf("invalid item body length")
		}
		var itemId uint8 = msg[offset]
		decMsg.items[itemId] = string(
			msg[offset+dmq.IDMsgItemHeaderSize : offset+dmq.IDMsgItemHeaderSize+itemLen])
		offset += dmq.IDMsgItemHeaderSize + itemLen
	}

	return &decMsg, nil
}

func StartPubTCP(bind string) {
	log.Info("start tcp listening for indexnode on: %s", bind)
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
		inCli := &IdxNodeClient{
			expire:     time.Now().Unix() + DfltExpire,
			conn:       conn,
			processBuf: make([]byte, Config.TCPRecvBufSize*2),
			processEnd: 0,
		}
		rc := recvTcpBufCache.Get()
		go handleTCPConn(inCli, rc)
	}
}

func setIdxNodeTimeout(cli *IdxNodeClient) error {
	var timeout time.Time = time.Now()
	timeout = timeout.Add(time.Second * time.Duration(DfltExpire))
	return cli.conn.SetReadDeadline(timeout)
}

func handleTCPConn(cli *IdxNodeClient, rc chan *bufio.Reader) {
	addr := cli.conn.RemoteAddr().String()
	log.Debug("handleTcpConn(%s) routine start", addr)

	for {
		if err := setIdxNodeTimeout(cli); err != nil {
			log.Error("IdxNodeClient set timeout error(%v)", err)
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

func processReadbuf(cli *IdxNodeClient, buf []byte) error {
	var remaining uint16 = uint16(len(buf))
	for {
		if remaining == 0 {
			cli.processEnd = 0
			return nil
		}
		if remaining < dmq.IDMsgHeaderSize {
			cli.processEnd = int(remaining)
			copy(buf[:cli.processEnd], buf[len(buf)-int(remaining):])
			return ProcessLater
		}
		start := uint16(len(buf)) - remaining
		var cmd uint8 = buf[start]
		var bodyLen uint16 = binary.BigEndian.Uint16(buf[start+dmq.IDMsgCmdSize:])
		if bodyLen > dmq.IDMsgMaxBodyLen {
			cli.processEnd = 0
			log.Error("invalid request, invalid body length: %d", bodyLen)
			return fmt.Errorf("invalid msg body len")
		}
		if remaining >= dmq.IDMsgHeaderSize+bodyLen {
			decMsg, err := binaryMsgDecode(buf[start:], bodyLen)
			remaining -= (dmq.IDMsgHeaderSize + bodyLen)
			if err != nil {
				log.Error("invalid request error(%v)", err)
				continue
			}
			if processFunc, ok := PubCmdTable[cmd]; ok {
				if err := processFunc.validate(decMsg); err != nil {
					log.Error("cmd %d request valid error(%v)", cmd, err)
				} else {
					if perr := processFunc.process(decMsg, cli); perr != nil {
						log.Error("process error: %v", perr)
					}
				}
			} else {
				log.Error("cmd: %d not support", cmd)
			}
		} else {
			cli.processEnd = int(remaining)
			copy(buf[:cli.processEnd], buf[len(buf)-int(remaining):])
			return ProcessLater
		}
	}
	cli.processEnd = 0
	return nil
}

func validatePushMsg(msg *DecodedMsg) error {
	if _, ok := msg.items[dmq.IDMsgItemPayloadId]; !ok {
		return fmt.Errorf("msg payload item not found")
	}

	if _, ok := msg.items[dmq.IDMsgItemAttributeId]; !ok {
		return fmt.Errorf("msg attribute item not found")
	}

	if _, ok := msg.items[dmq.IDMsgClientListIdId]; !ok {
		return fmt.Errorf("msg client id list item not found")
	}

	return nil
}

// cliId is in BSON format, not hex string
func checkSubCliMatchingMsg(pattrs []*PubAttr, cliId string) bool {
	if cli, ok := ClisInfo[cliId]; !ok {
		log.Warning("subclient %s not found in ClisInfo",
			hex.EncodeToString([]byte(cliId)))
		return false
	} else {
		for _, pattr := range pattrs {
			if attr, ok := cli.AttrMap[pattr.name]; !ok {
				// ignore if the subclient has no interest on this attribute
			} else {
				if int(attr.use) != pattr.use {
					log.Warning("different attr type for cli %s: %d expetted %d",
						hex.EncodeToString([]byte(cliId)), attr.use, pattr.use)
					return false
				}
				if pattr.val < attr.low || pattr.val > attr.high {
					return false
				}
			}
		}
	}
	return true
}

func extractMsgAttr(msg *DecodedMsg) ([]*PubAttr, error) {
	// Message must have been validated
	attrs := msg.items[dmq.IDMsgItemAttributeId]
	parsedAttrrs := make(map[string]interface{})
	if err := json.Unmarshal([]byte(attrs), &parsedAttrrs); err != nil {
		return nil, err
	}
	pattrs := make([]*PubAttr, 0)
	for aname, attr := range parsedAttrrs {
		if v, ok := attr.(string); ok {
			pattrs = append(pattrs, &PubAttr{
				name:   aname,
				use:    dmq.AttrUseField[dmq.AttrUseStr],
				strval: v,
			})
		}
		if v, ok := attr.(float64); ok {
			pattrs = append(pattrs, &PubAttr{
				name: aname,
				use:  dmq.AttrUseField[dmq.AttrUseRange],
				val:  v,
			})
		}
	}
	return pattrs, nil
}

func chooseMaxSubCliNum(msg *DecodedMsg) int {
	tmsg := &BasicMsg{
		cmdType: dmq.MDMsgCmdPushMsg,
		bodyLen: 0,
		extra:   dmq.MDMsgExtraNone,
		items: map[uint8]string{
			dmq.MDMsgItemMsgidId:   string(bson.NewObjectId()),
			dmq.MDMsgItemPayloadId: msg.items[dmq.IDMsgItemPayloadId],
		},
	}
	bmsg := binaryMsgEncode(tmsg)
	oneIdSize := dmq.SubClientIdSize + dmq.ConnectorNodeIdSize +
		int(dmq.IDMsgItemIdSize+dmq.IDMsgItemHeaderSize)
	return (int(dmq.MDMsgMaxBodyLen+dmq.MDMsgHeaderSize) - len(bmsg)) / oneIdSize
}

func processPushMsg(msg *DecodedMsg, cli *IdxNodeClient) error {
	// Message must have been validated before processing

	pattrs, err := extractMsgAttr(msg)
	if err != nil {
		return err
	}

	bits := dmq.SubClientIdSize
	bclis := []byte(msg.items[dmq.IDMsgClientListIdId])
	candClis := make([][]byte, 0)
	for i := 0; (i + bits) <= len(bclis); i += bits {
		if checkSubCliMatchingMsg(pattrs, string(bclis[i:i+bits])) {
			candClis = append(candClis, bclis[i:i+bits])
		}
	}

	maxclis := chooseMaxSubCliNum(msg)
	idx, clinum := 0, len(candClis)
	for idx < clinum {
		var end int
		if (idx + maxclis) < clinum {
			end = idx + maxclis
		} else {
			end = clinum
		}
		cliIdList := make([]byte, 0)
		for i := idx; i < end; i++ {
			cliId := candClis[i]
			cli, ok := ClisInfo[string(cliId)]
			if !ok {
				log.Error("subclient %s not in ClisInfo", hex.EncodeToString(cliId))
				continue
			}
			cliIdList = append(cliIdList, cliId...)
			cliIdList = append(cliIdList, []byte(cli.ConnId)...)
		}

		sendmsg := &BasicMsg{
			cmdType: dmq.MDMsgCmdPushMsg,
			bodyLen: 0,
			extra:   dmq.MDMsgExtraNone,
			items: map[uint8]string{
				dmq.MDMsgItemMsgidId:   string(bson.NewObjectId()),
				dmq.MDMsgItemPayloadId: msg.items[dmq.IDMsgItemPayloadId],
				dmq.MDMsgItemSubListId: string(cliIdList),
			},
		}
		bmsg := binaryMsgEncode(sendmsg)

		log.Debug("send msg: %v to disp (with %d subclis)",
			bmsg, len(cliIdList)/(dmq.SubClientIdSize+dmq.ConnectorNodeIdSize))

		if err := DispMsgSender(CurDispNode, bmsg); err != nil {
			log.Error("sendmsg to dispnode error: %v", err)
		}

		idx = end
	}

	return nil
}

func validateHeartbeat(msg *DecodedMsg) error {
	return nil
}

func processHeartbeat(msg *DecodedMsg, cli *IdxNodeClient) error {
	log.Debug("recv heartbeat %v from indexnode", msg)
	setIdxNodeTimeout(cli)
	cli.conn.Write([]byte("heartbeat received"))
	return nil
}
