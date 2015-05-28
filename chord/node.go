package chord

import (
	"bufio"
	"bytes"
	"encoding/base64"
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

// StatusClient manages the TCP connection from local serf status client
type StatusClient struct {
	hostname   string
	expire     int64
	conn       net.Conn
	processBuf []byte
	processEnd int
}

// BasicMsg struct
type BasicMsg struct {
	cmdType uint8
	bodyLen uint16
	extra   uint8
	items   map[uint8]string
}

// DecodedMsg struct
type DecodedMsg struct {
	extra   uint8
	bodyLen uint16
	items   map[uint8]string
}

var (
	dfltExpire int64 = 15 * 60
)

type handleMsgFunc struct {
	validate func(msg *DecodedMsg, n *Node) error
	process  func(msg *DecodedMsg, n *Node) error
}

var statusCmdTable = map[uint8]handleMsgFunc{
	dmq.SDDMsgCmdNodeInfo:  handleMsgFunc{validate: validateNodeInfoMsg, process: processNodeInfoMsg},
	dmq.SDDMsgCmdVNodeInfo: handleMsgFunc{validate: validateVNodeInfoMsg, process: processVNodeInfoMsg},
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
	w.WriteString(fmt.Sprintf("[%s]\n", cfgSectNode))
	rpcAddr := strings.Split(conf.RPCAddr, ":")
	rpcPort := rpcAddr[len(rpcAddr)-1]
	w.WriteString(fmt.Sprintf("%s = %s:%s\n", cfgItemRpcaddr, conf.HostIP, rpcPort))
	w.Flush()
	return nil
}

// CreateNode creates a new Node struct
func CreateNode(conf *NodeConfig) *Node {
	node := &Node{
		Config: conf,
		Rtable: &RTable{
			vnodes: make([]*Vnode, 0),
			peers:  make([]*PeerNode, 0),
		},
	}
	node.init()
	return node
}

func (n *Node) init() {
	n.LVnodes = make([]*localVnode, n.Config.NumVnodes)

	// change working dir
	chgWorkdir(n.Config.WorkDir)

	createSerfevHelper(n.Config)

	peerNode := &PeerNode{
		Hostname:  n.Config.Hostname,
		SerfNode:  n.Config.Serf.NodeName,
		BindAddr:  n.Config.BindAddr,
		RPCAddr:   n.Config.RPCAddr,
		StartHash: n.Config.StartHash,
	}
	n.Rtable.peers = append(n.Rtable.peers, peerNode)
	curHash := n.Config.StartHash[:]
	for i := 0; i < n.Config.NumVnodes; i++ {
		lvn := &localVnode{
			node: n,
		}
		n.LVnodes[i] = lvn
		lvn.init(peerNode, curHash)
		n.Rtable.joinVnode(&lvn.Vnode)
		curHash = HashJump(curHash, n.Config.step, n.Config.maxhash)
	}
}

// SetLogger is used to set logging module for this node
func (n *Node) SetLogger(log *logging.Logger) {
	n.log = log
}

// Len is the number of vnodes
func (n *Node) Len() int {
	return len(n.LVnodes)
}

// Less returns whether the vnode with index i should sort
// before the vnode with index j.
func (n *Node) Less(i, j int) bool {
	return bytes.Compare(n.LVnodes[i].ID, n.LVnodes[j].ID) == -1
}

// Swap swaps the vnodes with indexes i and j.
func (n *Node) Swap(i, j int) {
	n.LVnodes[i], n.LVnodes[j] = n.LVnodes[j], n.LVnodes[i]
}

// SerfStart is a wrapper function for serfStart
func (n *Node) SerfStart(c chan Notification, logger io.Writer) {
	params := make(map[string]string)
	if n.Config.Serf.NodeName != "" {
		params["node"] = n.Config.Serf.NodeName
	}
	if n.Config.Serf.BindAddr != "" {
		params["bind"] = n.Config.Serf.BindAddr
	}
	if n.Config.Serf.RPCAddr != "" {
		params["rpc-addr"] = n.Config.Serf.RPCAddr
	}
	if n.Config.Serf.EvHandler != "" {
		params["event-handler"] = n.Config.Serf.EvHandler
	}
	if n.Config.Serf.ConfigFile != "" {
		params["config-file"] = n.Config.Serf.ConfigFile
	}

	serfStart(c, logger, n.Config.Serf.BinPath, params, n.Config.Serf.Args)
}

// SerfStop is a wrapper function for serfStop
func (n *Node) SerfStop() error {
	return serfStop(n.Config.Serf.BinPath, n.Config.Serf.RPCAddr)
}

// SerfJoin is called to tell serf running on this node to join the cluster
// addr is an arbitrary serf bind address of nodes in Chord ring
func (n *Node) SerfJoin(addr string) error {
	return serfJoin(n.Config.Serf.BinPath, n.Config.Serf.RPCAddr, addr)
}

// SerfUserEvent is Called to send serf user event to serf cluster
// evname, represents for event type, including:
// 	 nodeinfo: information of chord physical node
// 	 vnodeinfo: information of all vnodes that belongs to one chord physical node
// coalesce:
//   If coalesce is true, if many events of the same name are received within a
//   short amount of time, the event handler is only invoked once.
func (n *Node) SerfUserEvent(evname, payload string, coalesce bool, c chan Notification) {
	params := make(map[string]string)
	params["coalesce"] = "false"
	if n.Config.Serf.RPCAddr != "" {
		params["rpc-addr"] = n.Config.Serf.RPCAddr
	}
	serfUserEvent(n.Config.Serf.BinPath, evname, payload, params, c)
}

// Shutdown is called when we want to destroy the node
func (n *Node) Shutdown() error {
	err := n.SerfStop()
	return err
}

// SerfSchdule is called after one chord node is created and to create or join
// a serf topology
func (n *Node) SerfSchdule(c chan Notification, logger io.Writer) error {
	// start serf agent
	n.SerfStart(c, logger)

	// try to detect member's aliveness for three times
	retryConut := 3
	for i := 0; i < retryConut; i++ {
		msg, err := checkMemberAlive(n.Config.Serf.BinPath, n.Config.Serf.RPCAddr)
		if err == nil {
			if strings.Contains(msg, n.Config.Serf.NodeName) &&
				strings.Contains(msg, serfAgentAlive) {
				break
			}
		}
		if i == retryConut-1 {
			if err != nil {
				return fmt.Errorf("%v: %s", err, msg)
			}
			return fmt.Errorf("alive not detected: %s", msg)
		}
	}

	// if not the first launched serf agent, join the cluster
	if n.Config.Entrypoint != "" {
		if err := n.SerfJoin(n.Config.Entrypoint); err != nil {
			return err
		}
	}

	// broadcast node information via serf
	info, err := n.Nodeinfo()
	if err != nil {
		return err
	}
	n.SerfUserEvent(serfUserevNodeinfo, string(info), false, c)

	// broadcast node's virtual nodes information via serf
	vinfo, err := n.Vnodeinfo()
	if err != nil {
		return err
	}
	n.SerfUserEvent(serfUserevVnodeinfo, string(vinfo), false, c)

	return nil
}

// Nodeinfo returns a json string represents chord node information
func (n *Node) Nodeinfo() ([]byte, error) {
	info := make(map[string]string)
	info["hostname"] = n.Config.Hostname
	info["serf"] = n.Config.Serf.NodeName
	info["bindaddr"] = n.Config.BindAddr
	info["rpcaddr"] = n.Config.RPCAddr
	info["starthash"] = hex.EncodeToString(n.Config.StartHash)

	return json.Marshal(&info)
}

// Vnodeinfo returns node information with all its vnodes in json string format
// TODO: Support vnode information broadcasting with any packet size.
// Currently we don't support broadcasting vnode info larger than 512 byte.
// because serf has data size limitation with UserEventSizeLimit = 512 byte
func (n *Node) Vnodeinfo() ([]byte, error) {
	info := make(map[string][]byte)
	info["hostname"] = []byte(n.Config.Hostname)
	info["serf"] = []byte(n.Config.Serf.NodeName)
	var ids []byte
	for _, vnode := range n.LVnodes {
		ids = append(ids, vnode.ID...)
	}
	info["vnode"] = ids

	return json.Marshal(&info)
}

// StartStatusTcp is the public interface for node/vnode information and status
// broadcasting
func (n *Node) StartStatusTcp() {
	go n.statusTcpListen()
}

func (n *Node) statusTcpListen() {
	log := n.log
	bind := n.Config.RPCAddr
	addr, err := net.ResolveTCPAddr("tcp", bind)
	if err != nil {
		log.Error("net.ResolveTCPAddr(%s) error", bind)
		panic(err)
	}

	l, err := net.ListenTCP("tcp4", addr)
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
	recvTcpBufCache := dmq.NewTcpBufCache(n.Config.TCPBufInsNum, n.Config.TCPBufioNum)
	for {
		conn, err := l.AcceptTCP()
		if err != nil {
			log.Error("listener.AcceptTCP() error(%v)", err)
			continue
		}
		if err = conn.SetReadBuffer(n.Config.TCPRecvBufSize * 2); err != nil {
			log.Error("conn.SetReadBuffer(%d) error(%v)",
				n.Config.TCPRecvBufSize, err)
			conn.Close()
			continue
		}
		if err = conn.SetWriteBuffer(n.Config.TCPSendBufSize * 2); err != nil {
			log.Error("conn.SetWriteBuffer(%d) error(%v)",
				n.Config.TCPSendBufSize, err)
			conn.Close()
			continue
		}
		statusCli := &StatusClient{
			expire:     time.Now().Unix() + dfltExpire,
			conn:       conn,
			processBuf: make([]byte, n.Config.TCPRecvBufSize*2),
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
		timeout := time.Now().Add(time.Second * time.Duration(dfltExpire))
		if err := cli.conn.SetReadDeadline(timeout); err != nil {
			log.Error("StatusClient set timeout error(%v)", err)
			break
		}
		rd := dmq.NewBufioReader(rc, cli.conn, n.Config.TCPRecvBufSize)
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
			err := n.processReadbuf(cli, cli.processBuf[:cli.processEnd+rlen])
			if err != nil && err != errProcessLater {
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
	remaining := uint16(len(buf))
	for {
		if remaining == 0 {
			cli.processEnd = 0
			return nil
		}
		if remaining < dmq.SDDMsgHeaderSize {
			cli.processEnd = int(remaining)
			copy(buf[:cli.processEnd], buf[len(buf)-int(remaining):])
			return errProcessLater
		}
		start := uint16(len(buf)) - remaining
		cmd := buf[start]
		bodyLen := binary.BigEndian.Uint16(buf[start+dmq.SDDMsgCmdSize:])
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
			if processFunc, ok := statusCmdTable[cmd]; ok {
				if err := processFunc.validate(decMsg, n); err != nil {
					log.Error("cmd %d request valid error(%v)", cmd, err)
				} else {
					if processErr := processFunc.process(decMsg, n); processErr != nil {
						log.Error("process error: %v", processErr)
					}
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
	extra := msg[dmq.SDDMsgCmdSize+dmq.SDDMsgBodySize]
	decMsg := DecodedMsg{
		extra: extra, bodyLen: bodyLen, items: make(map[uint8]string, 0),
	}

	totalLen := dmq.SDDMsgHeaderSize + bodyLen
	offset := dmq.SDDMsgHeaderSize
	for offset < totalLen {
		if offset+dmq.SDDMsgItemHeaderSize > totalLen {
			return nil, fmt.Errorf("invalid item header length")
		}
		itemLen := binary.BigEndian.Uint16(msg[offset+dmq.SDDMsgItemIDSize:])
		if itemLen+dmq.SDDMsgItemHeaderSize+offset > totalLen {
			return nil, fmt.Errorf("invalid item body length")
		}
		itemID := msg[offset]
		decMsg.items[itemID] = string(
			msg[offset+dmq.SDDMsgItemHeaderSize : offset+dmq.SDDMsgItemHeaderSize+itemLen])
		offset += dmq.SDDMsgItemHeaderSize + itemLen
	}

	return &decMsg, nil
}

func validateNodeInfoMsg(msg *DecodedMsg, n *Node) error {
	if _, ok := msg.items[dmq.SDDMsgItemHostnameID]; !ok {
		return fmt.Errorf("msg hostname item not found")
	}

	if _, ok := msg.items[dmq.SDDMsgItemSerfNodeID]; !ok {
		return fmt.Errorf("msg serfnode item not found")
	}

	if _, ok := msg.items[dmq.SDDMsgItemBindAddrID]; !ok {
		return fmt.Errorf("msg bindaddr item not found")
	}

	if _, ok := msg.items[dmq.SDDMsgItemRPCAddrID]; !ok {
		return fmt.Errorf("msg rpcaddr item not found")
	}

	if _, ok := msg.items[dmq.SDDMsgItemStartHashID]; !ok {
		return fmt.Errorf("msg starthash item not found")
	}
	return nil
}

func processNodeInfoMsg(msg *DecodedMsg, n *Node) error {
	log := n.log

	// Message must have been validated before processing
	hostname := msg.items[dmq.SDDMsgItemHostnameID]

	log.Debug("recv nodeinfo msg: %v from %s", msg, hostname)

	_, peer := n.Rtable.FindPeer(hostname)
	if peer != nil {
		// TODO: update both peer information and vnodes information
	} else {
		// new peer node
		peerNode := &PeerNode{
			Hostname:  hostname,
			SerfNode:  msg.items[dmq.SDDMsgItemSerfNodeID],
			BindAddr:  msg.items[dmq.SDDMsgItemBindAddrID],
			RPCAddr:   msg.items[dmq.SDDMsgItemRPCAddrID],
			StartHash: []byte(msg.items[dmq.SDDMsgItemStartHashID]),
		}
		n.Rtable.peers = append(n.Rtable.peers, peerNode)

		// send node and vnode info directly to newly joined node
		go n.sendNodeInfo(peerNode.RPCAddr)
		go n.sendVnodeInfo(peerNode.RPCAddr)
	}

	return nil
}

func validateVNodeInfoMsg(msg *DecodedMsg, n *Node) error {
	if _, ok := msg.items[dmq.SDDMsgItemHostnameID]; !ok {
		return fmt.Errorf("msg hostname item not found")
	}

	if _, ok := msg.items[dmq.SDDMsgItemSerfNodeID]; !ok {
		return fmt.Errorf("msg serfnode item not found")
	}

	if _, ok := msg.items[dmq.SDDMsgItemVNodeListID]; !ok {
		return fmt.Errorf("msg vnode list item not found")
	}
	return nil
}

func processVNodeInfoMsg(msg *DecodedMsg, n *Node) error {
	log := n.log

	// Message must have been validated before processing
	hostname, err :=
		base64.StdEncoding.DecodeString(msg.items[dmq.SDDMsgItemHostnameID])
	if err != nil {
		log.Error("failed to parse hostname %v", err)
		return err
	}

	vnodes, err :=
		base64.StdEncoding.DecodeString(msg.items[dmq.SDDMsgItemVNodeListID])
	if err != nil {
		log.Error("failed to parse vnodelist %v", err)
		return err
	}
	var vnodeIDs [][]byte
	for i := 0; i < len(vnodes); i += n.Config.HashBits / 8 {
		if i+n.Config.HashBits/8 <= len(vnodes) {
			vnodeIDs = append(vnodeIDs, vnodes[i:i+20])
		}
	}

	log.Debug("recv vnodeinfo from %s", hostname)

	var peer *PeerNode
	retryConut := 5
	for i := 0; i < retryConut; i++ {
		_, peer = n.Rtable.FindPeer(string(hostname))
		if peer == nil {
			// wait a short time
			log.Warning("waiting for node %s's nodeinfo", string(hostname))
			time.Sleep(time.Millisecond * time.Duration(100))
			_, peer = n.Rtable.FindPeer(string(hostname))
		} else {
			break
		}
	}

	if peer == nil {
		return fmt.Errorf("peer %s not found", string(hostname))
	}

	for _, nid := range vnodeIDs {
		vnode := &Vnode{
			ID:    nid,
			Pnode: peer,
		}
		n.Rtable.joinVnode(vnode)
	}

	return nil
}

func (n *Node) sendInfoDirect(info []byte, rpcAddr string) error {
	raddr, err := net.ResolveTCPAddr("tcp", rpcAddr)
	if err != nil {
		return err
	}

	conn, err := net.DialTCP("tcp", nil, raddr)
	if err != nil {
		return err
	}

	_, err = conn.Write(info)
	return err
}

func binaryMsgEncode(msg *BasicMsg) []byte {
	bmsg := make([]byte, dmq.DRMsgHeaderSize)
	bmsg[0] = msg.cmdType
	binary.BigEndian.PutUint16(bmsg[1:], msg.bodyLen)
	bmsg[dmq.SDDMsgCmdSize+dmq.SDDMsgBodySize] = msg.extra
	var bodyLen uint16
	for itemid, item := range msg.items {
		bmsg = append(bmsg, itemid)
		bItemLen := make([]byte, dmq.SDDMsgItemBodySize)
		binary.BigEndian.PutUint16(bItemLen, uint16(len(item)))
		bmsg = append(bmsg, bItemLen...)
		bmsg = append(bmsg, item...)
		bodyLen += dmq.SDDMsgItemHeaderSize + uint16(len(item))
	}
	if msg.bodyLen != bodyLen {
		binary.BigEndian.PutUint16(bmsg[1:], bodyLen)
	}
	return bmsg
}

// send node information directly to a chord node via RPCBind address
func (n *Node) sendNodeInfo(rpcAddr string) {
	log := n.log
	basicMsg := &BasicMsg{
		cmdType: dmq.SDDMsgCmdNodeInfo,
		bodyLen: 0,
		extra:   0,
		items: map[uint8]string{
			dmq.SDDMsgItemHostnameID:  n.Config.Hostname,
			dmq.SDDMsgItemBindAddrID:  n.Config.BindAddr,
			dmq.SDDMsgItemRPCAddrID:   n.Config.RPCAddr,
			dmq.SDDMsgItemSerfNodeID:  n.Config.Serf.NodeName,
			dmq.SDDMsgItemStartHashID: hex.EncodeToString(n.Config.StartHash),
		},
	}
	bmsg := binaryMsgEncode(basicMsg)
	if err := n.sendInfoDirect(bmsg, rpcAddr); err != nil {
		log.Error("send node info with error: %v", err)
	}
}

// send vritual node information directly to a chord node via RPCBind address
func (n *Node) sendVnodeInfo(rpcAddr string) {
	log := n.log

	var ids []byte
	for _, vnode := range n.LVnodes {
		ids = append(ids, vnode.ID...)
	}
	basicMsg := &BasicMsg{
		cmdType: dmq.SDDMsgCmdVNodeInfo,
		bodyLen: 0,
		extra:   0,
		items: map[uint8]string{
			dmq.SDDMsgItemHostnameID:  base64.StdEncoding.EncodeToString([]byte(n.Config.Hostname)),
			dmq.SDDMsgItemSerfNodeID:  base64.StdEncoding.EncodeToString([]byte(n.Config.Serf.NodeName)),
			dmq.SDDMsgItemVNodeListID: base64.StdEncoding.EncodeToString(ids),
		},
	}
	bmsg := binaryMsgEncode(basicMsg)
	if err := n.sendInfoDirect(bmsg, rpcAddr); err != nil {
		log.Error("send vnode info with error: %v", err)
	}
}

// Deprecated: Public interface for messages delivery
func (n *Node) startMsgTcp(log *logging.Logger) {
	go n.msgTcpListen(log)
}

func (n *Node) msgTcpListen(log *logging.Logger) {
}
