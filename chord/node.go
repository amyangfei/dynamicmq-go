package chord

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"github.com/op/go-logging"
	"io"
	"net"
	"os"
	"strings"
	"time"
)

type StatusClient struct {
	hostname   string
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

var (
	DfltExpire int64 = 15 * 60
)

type HandleMsgFunc struct {
	validate func(msg *DecodedMsg, n *Node) error
	process  func(msg *DecodedMsg, n *Node) error
}

var StatusCmdTable = map[uint8]HandleMsgFunc{
	dmq.SDDMsgCmdNodeInfo:  HandleMsgFunc{validate: validateNodeInfoMsg, process: processNodeInfoMsg},
	dmq.SDDMsgCmdVNodeInfo: HandleMsgFunc{validate: validateVNodeInfoMsg, process: processVNodeInfoMsg},
}

func chgWorkdir(path string) error {
	if path == "" {
		return nil
	}
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			os.MkdirAll(path, 0755)
		}
	}
	return os.Chdir(path)
}

func createSerfevHelper(conf *NodeConfig) error {
	fname := fmt.Sprintf("%s.evhelper.ini", conf.Serf.NodeName)
	f, err := os.OpenFile(fname, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriter(f)
	w.WriteString(fmt.Sprintf("[%s]\n", cfg_sect_node))
	w.WriteString(fmt.Sprintf("%s = %s\n", cfg_item_rpcaddr, conf.RPCAddr))
	w.Flush()
	return nil
}

func CreateNode(conf *NodeConfig) *Node {
	node := &Node{config: conf}
	node.init()
	return node
}

func (n *Node) init() {
	n.Vnodes = make([]*localVnode, n.config.NumVnodes)

	// change working dir
	chgWorkdir(n.config.WorkDir)

	createSerfevHelper(n.config)

	curHash := n.config.StartHash[:]
	for i := 0; i < n.config.NumVnodes; i++ {
		lvn := &localVnode{
			node: n,
		}
		n.Vnodes[i] = lvn
		lvn.init(curHash)
		curHash = HashJump(curHash, n.config.step, n.config.maxhash)
	}
}

func (n *Node) SetLogger(log *logging.Logger) {
	n.log = log
}

// Len is the number of vnodes
func (n *Node) Len() int {
	return len(n.Vnodes)
}

// Less returns whether the vnode with index i should sort
// before the vnode with index j.
func (n *Node) Less(i, j int) bool {
	return bytes.Compare(n.Vnodes[i].Id, n.Vnodes[j].Id) == -1
}

// Swap swaps the vnodes with indexes i and j.
func (n *Node) Swap(i, j int) {
	n.Vnodes[i], n.Vnodes[j] = n.Vnodes[j], n.Vnodes[i]
}

func (n *Node) SerfStart(c chan Notification, logger io.Writer) {
	params := make(map[string]string)
	if n.config.Serf.NodeName != "" {
		params["node"] = n.config.Serf.NodeName
	}
	if n.config.Serf.BindAddr != "" {
		params["bind"] = n.config.Serf.BindAddr
	}
	if n.config.Serf.RPCAddr != "" {
		params["rpc-addr"] = n.config.Serf.RPCAddr
	}
	if n.config.Serf.EvHandler != "" {
		params["event-handler"] = n.config.Serf.EvHandler
	}
	if n.config.Serf.ConfigFile != "" {
		params["config-file"] = n.config.Serf.ConfigFile
	}

	serfStart(c, logger, n.config.Serf.BinPath, params, n.config.Serf.Args)
}

func (n *Node) SerfStop() error {
	return serfStop(n.config.Serf.BinPath, n.config.Serf.RPCAddr)
}

// Call This function to tell serf running on this node to join the cluster
// addr is an arbitrary serf bind address of nodes in Chord ring
func (n *Node) SerfJoin(addr string) error {
	return serfJoin(n.config.Serf.BinPath, n.config.Serf.RPCAddr, addr)
}

// Call This function to send serf user event to serf cluster
// evname, represents for event type, including:
// 	 nodeinfo: information of chord physical node
// 	 vnodeinfo: information of all vnodes that belongs to one chord physical node
// coalesce:
//   If coalesce is true, if many events of the same name are received within a
//   short amount of time, the event handler is only invoked once.
func (n *Node) SerfUserEvent(evname, payload string, coalesce bool, c chan Notification) {
	params := make(map[string]string)
	params["coalesce"] = "false"
	if n.config.Serf.RPCAddr != "" {
		params["rpc-addr"] = n.config.Serf.RPCAddr
	}
	serfUserEvent(n.config.Serf.BinPath, evname, payload, params, c)
}

func (n *Node) Shutdown() error {
	err := n.SerfStop()
	return err
}

func (n *Node) serfSchdule(c chan Notification, logger io.Writer) error {
	// start serf agent
	n.SerfStart(c, logger)

	// try to detect member's aliveness for three times
	retry_count := 3
	for i := 0; i < retry_count; i++ {
		msg, err := checkMemberAlive(n.config.Serf.BinPath, n.config.Serf.RPCAddr)
		if err == nil {
			if strings.Contains(msg, n.config.Serf.NodeName) &&
				strings.Contains(msg, serf_agent_alive) {
				break
			}
		}
		if i == retry_count-1 {
			if err != nil {
				return fmt.Errorf("%v: %s", err, msg)
			} else {
				return fmt.Errorf("alive not detected: %s", msg)
			}
		}
	}

	// if not the first launched serf agent, join the cluster
	if n.config.Entrypoint != "" {
		if err := n.SerfJoin(n.config.Entrypoint); err != nil {
			return err
		}
	}

	// broadcast node information
	if info, err := n.Nodeinfo(); err != nil {
		return err
	} else {
		n.SerfUserEvent(serf_userev_nodeinfo, string(info), false, c)
	}

	// broadcast node's virtual nodes information
	if info, err := n.Vnodeinfo(); err != nil {
		return err
	} else {
		n.SerfUserEvent(serf_userev_vnodeinfo, string(info), false, c)
	}

	return nil
}

// return a json string represents chord node information
func (n *Node) Nodeinfo() ([]byte, error) {
	info := make(map[string]string)
	info["hostname"] = n.config.Hostname
	info["bindaddr"] = n.config.BindAddr
	info["rpcaddr"] = n.config.RPCAddr
	info["starthash"] = hex.EncodeToString(n.config.StartHash)

	return json.Marshal(&info)
}

// TODO: Support vnode information broadcasting with any packet size.
// Currently we don't support broadcasting vnode info larger than 512 byte.
// because serf has data size limitation with UserEventSizeLimit = 512 byte
func (n *Node) Vnodeinfo() ([]byte, error) {
	info := make(map[string][]byte)
	info["hostname"] = []byte(n.config.Hostname)
	ids := make([]byte, 0)
	for _, vnode := range n.Vnodes {
		ids = append(ids, vnode.Id...)
	}
	info["vnode"] = ids

	return json.Marshal(&info)
}

// Public interface for node/vnode information and status broadcasting
// FIXME: use generic log interface
func (n *Node) StartStatusTcp() {
	go n.statusTcpListen()
}

func (n *Node) statusTcpListen() {
	log := n.log
	bind := n.config.BindAddr
	addr, err := net.ResolveTCPAddr("tcp", n.config.BindAddr)
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
	recvTcpBufCache := dmq.NewTcpBufCache(n.config.TCPBufInsNum, n.config.TCPBufioNum)
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
		if err = conn.SetReadBuffer(n.config.TCPRecvBufSize * 2); err != nil {
			log.Error("conn.SetReadBuffer(%d) error(%v)",
				n.config.TCPRecvBufSize, err)
			conn.Close()
			continue
		}
		if err = conn.SetWriteBuffer(n.config.TCPSendBufSize * 2); err != nil {
			log.Error("conn.SetWriteBuffer(%d) error(%v)",
				n.config.TCPSendBufSize, err)
			conn.Close()
			continue
		}
		statusCli := &StatusClient{
			expire:     time.Now().Unix() + DfltExpire,
			conn:       conn,
			processBuf: make([]byte, n.config.TCPRecvBufSize*2),
			processEnd: 0,
		}
		rc := recvTcpBufCache.Get()
		go n.handleStatusTCPconn(statusCli, rc)
	}
}

func (n *Node) handleStatusTCPconn(cli *StatusClient, rc chan *bufio.Reader) {
	log := n.log
	addr := cli.conn.RemoteAddr().String()
	log.Debug("handleStatusTCPconn(%s) routine start", addr)

	for {
		timeout := time.Now().Add(time.Second * time.Duration(DfltExpire))
		if err := cli.conn.SetReadDeadline(timeout); err != nil {
			log.Error("StatusClient set timeout error(%v)", err)
			break
		}
		rd := dmq.NewBufioReader(rc, cli.conn, n.config.TCPRecvBufSize)
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
			err := n.processReadbuf(cli, cli.processBuf[:cli.processEnd+rlen])
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
	log.Debug("addr: %s handleStatusTCPconn routine stop", addr)
}

func (n *Node) processReadbuf(cli *StatusClient, buf []byte) error {
	log := n.log
	var remaining uint16 = uint16(len(buf))
	for {
		if remaining == 0 {
			return nil
		}
		if remaining < dmq.SDDMsgHeaderSize {
			return ProcessLater
		}
		start := uint16(len(buf)) - remaining
		var cmd uint8 = buf[start]
		var bodyLen uint16 = binary.BigEndian.Uint16(buf[start+dmq.SDDMsgCmdSize:])
		if bodyLen > dmq.SDDMsgMaxBodyLen {
			cli.processEnd = 0
			log.Error("invalid request, invalid body length: %d", bodyLen)
			return fmt.Errorf("invalid msg body len")
		}
		if remaining >= dmq.SDDMsgHeaderSize+bodyLen {
			decMsg, err := binaryMsgDecode(buf[start:], bodyLen)
			remaining -= (dmq.SDDMsgHeaderSize + bodyLen)
			if err != nil {
				log.Error("invalid request error(%v)", err)
				continue
			}
			if processFunc, ok := StatusCmdTable[cmd]; ok {
				if err := processFunc.validate(decMsg, n); err != nil {
					log.Error("cmd %d request valid error(%v)", cmd, err)
				} else {
					processFunc.process(decMsg, n)
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
	var extra uint8 = msg[dmq.SDDMsgCmdSize+dmq.SDDMsgBodySize]
	decMsg := DecodedMsg{
		extra: extra, bodyLen: bodyLen, items: make(map[uint8]string, 0),
	}

	totalLen := dmq.SDDMsgHeaderSize + bodyLen
	offset := dmq.SDDMsgHeaderSize
	for offset < totalLen {
		if offset+dmq.SDDMsgItemHeaderSize > totalLen {
			return nil, fmt.Errorf("invalid item header length")
		}
		itemLen := binary.BigEndian.Uint16(msg[offset+dmq.SDDMsgItemIdSize:])
		if itemLen+dmq.SDDMsgItemHeaderSize+offset > totalLen {
			return nil, fmt.Errorf("invalid item body length")
		}
		var itemId uint8 = msg[offset]
		decMsg.items[itemId] = string(
			msg[offset+dmq.SDDMsgItemHeaderSize : offset+dmq.SDDMsgItemHeaderSize+itemLen])
		offset += dmq.SDDMsgItemHeaderSize + itemLen
	}

	return &decMsg, nil
}

func validateNodeInfoMsg(msg *DecodedMsg, n *Node) error {
	return nil
}

func processNodeInfoMsg(msg *DecodedMsg, n *Node) error {
	return nil
}

func validateVNodeInfoMsg(msg *DecodedMsg, n *Node) error {
	return nil
}

func processVNodeInfoMsg(msg *DecodedMsg, n *Node) error {
	return nil
}

// Public interface for messages delivery
func (n *Node) StartMsgTcp(log *logging.Logger) {
	go n.msgTcpListen(log)
}

func (n *Node) msgTcpListen(log *logging.Logger) {
}
