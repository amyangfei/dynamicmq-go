package main

import (
	"encoding/binary"
	"encoding/hex"
	"flag"
	"fmt"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"github.com/coreos/go-etcd/etcd"
	"gopkg.in/mgo.v2/bson"
	"math/rand"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

var MsgFrequency = flag.Float64("r", 1.0, "msg sent frequency")
var CliHitPercent = flag.Int("p", 100, "perecent of clients to send msg")
var RunTime = flag.Int("t", 30, "run time in second")
var MetaEtcd = flag.String("a", "http://127.0.0.1:4001", "address of meta-storage")
var MaxCliPerMsg int

var SubClis []*SubClient

var DispNodes []*DispNode

type SubClient struct {
	id     string
	connid string
}

type DispNode struct {
	dispid string
	connid string
	baddr  string
	conn   net.Conn
}

type BasicMsg struct {
	cmdType uint8
	bodyLen uint16
	extra   uint8
	items   map[uint8]string
}

type DecodedMsg struct {
	extra   uint8
	bodyLen uint16
	items   map[uint8]string
}

func handleSignal(sigChan chan os.Signal) {
	for {
		s := <-sigChan
		fmt.Printf("receive a signal %s\n", s.String())
		switch s {
		case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGSTOP, syscall.SIGINT:
			// clean work
			os.Exit(0)
		default:
			return
		}
	}
}

func getEtcdKeyLastSep(key string) string {
	klist := strings.Split(key, "/")
	if len(klist) < 1 {
		return ""
	} else {
		return klist[len(klist)-1]
	}
}

func loadSubscribers(c *etcd.Client) {
	SubClis = make([]*SubClient, 0)

	subInfoBase := dmq.GetInfoBase(dmq.EtcdSubscriberType)
	resp, err := c.Get(subInfoBase, false, true)
	if err != nil {
		panic(err)
	} else if !resp.Node.Dir {
		panic(fmt.Errorf("%s should be directory", resp.Node.Key))
	}
	for _, sub := range resp.Node.Nodes {
		connIdKey := fmt.Sprintf("%s/%s", sub.Key, dmq.DispConnId)
		if connResp, err := c.Get(connIdKey, false, false); err != nil {
			panic(err)
		} else {
			cli := &SubClient{
				id:     getEtcdKeyLastSep(sub.Key),
				connid: connResp.Node.Value,
			}
			SubClis = append(SubClis, cli)
		}
	}
}

func loadDispInfo(c *etcd.Client) {
	DispNodes = make([]*DispNode, 0)

	dispInfoBase := dmq.GetInfoBase(dmq.EtcdDispatcherType)
	resp, err := c.Get(dispInfoBase, false, true)
	if err != nil {
		panic(err)
	}
	for _, disp := range resp.Node.Nodes {
		bindAddr, connId := "", ""
		bindAddrKey := fmt.Sprintf("%s/%s", disp.Key, dmq.DispBindAddr)
		if bindResp, err := c.Get(bindAddrKey, false, false); err != nil {
			panic(err)
		} else {
			bindAddr = bindResp.Node.Value
		}
		connIdKey := fmt.Sprintf("%s/%s", disp.Key, dmq.DispConnId)
		if connResp, err := c.Get(connIdKey, false, false); err != nil {
			panic(err)
		} else {
			connId = connResp.Node.Value
		}
		dnode := &DispNode{
			dispid: getEtcdKeyLastSep(disp.Key),
			connid: connId,
			baddr:  bindAddr,
			conn:   nil,
		}
		DispNodes = append(DispNodes, dnode)
	}
}

func binaryMsgEncode(msg *BasicMsg) []byte {
	bmsg := make([]byte, dmq.MDMsgHeaderSize)
	bmsg[0] = msg.cmdType
	binary.BigEndian.PutUint16(bmsg[1:], msg.bodyLen)
	bmsg[dmq.MDMsgCmdSize+dmq.MDMsgBodySize] = msg.extra
	var bodyLen uint16 = 0
	for itemid, item := range msg.items {
		bmsg = append(bmsg, itemid)
		bItemLen := make([]byte, dmq.MDMsgItemBodySize)
		binary.BigEndian.PutUint16(bItemLen, uint16(len(item)))
		bmsg = append(bmsg, bItemLen...)
		bmsg = append(bmsg, item...)
		bodyLen += dmq.MDMsgItemHeaderSize + uint16(len(item))
	}
	if msg.bodyLen != bodyLen {
		binary.BigEndian.PutUint16(bmsg[1:], bodyLen)
	}
	return bmsg
}

func chooseMaxSubCliNum() int {
	tmsg := &BasicMsg{
		cmdType: dmq.MDMsgCmdPushMsg,
		bodyLen: 0,
		extra:   dmq.MDMsgExtraNone,
		items: map[uint8]string{
			dmq.MDMsgItemMsgidId:   string(bson.NewObjectId()),
			dmq.MDMsgItemPayloadId: fmt.Sprintf("%d", time.Now().UnixNano()/1e3),
		},
	}
	bmsg := binaryMsgEncode(tmsg)
	oneIdSize := dmq.SubClientIdSize + dmq.ConnectorNodeIdSize +
		int(dmq.MDMsgItemIdSize+dmq.MDMsgItemHeaderSize)
	return (int(dmq.IDMsgMaxBodyLen+dmq.IDMsgHeaderSize) - len(bmsg)) / oneIdSize
}

func sendMsg(percent int) {
	disp := DispNodes[rand.Intn(len(DispNodes))]
	if disp.conn == nil {
		addr, err := net.ResolveTCPAddr("tcp", disp.baddr)
		if err != nil {
			panic(err)
		}
		conn, err := net.DialTCP("tcp", nil, addr)
		if err != nil {
			panic(err)
		}
		disp.conn = conn
		go func(disp *DispNode) {
			for {
				buf := make([]byte, 64)
				if _, err := disp.conn.Read(buf); err != nil {
					disp.conn.Close()
					disp.conn = nil
				}
			}
		}(disp)
	}

	idx := 0
	msgnums := int(float64(len(SubClis)) * float64(percent) / 100)
	if msgnums < len(SubClis) {
		idx = rand.Intn(len(SubClis) - msgnums)
		msgnums = idx + msgnums
	}
	for idx < msgnums {
		var end int
		if (idx + MaxCliPerMsg) < msgnums {
			end = idx + MaxCliPerMsg
		} else {
			end = msgnums
		}
		ids := make([]byte, 0)
		for i := idx; i < end; i++ {
			did, err := hex.DecodeString(SubClis[i].id)
			if err != nil {
				fmt.Printf("error format of subid %s", SubClis[i].id)
				continue
			}

			ids = append(ids, did...)
			ids = append(ids, []byte(SubClis[i].connid)...)
		}
		msg := &BasicMsg{
			cmdType: dmq.MDMsgCmdPushMsg,
			bodyLen: 0,
			extra:   dmq.MDMsgExtraNone,
			items: map[uint8]string{
				dmq.MDMsgItemMsgidId:   string(bson.NewObjectId()),
				dmq.MDMsgItemPayloadId: fmt.Sprintf("%d", time.Now().UnixNano()/1e3),
				dmq.MDMsgItemSubListId: string(ids),
			},
		}
		bmsg := binaryMsgEncode(msg)
		disp.conn.Write(bmsg)
		idx = end
	}
}

func dispBencher(hitPercent, runTime int, freq float64) {
	fmt.Printf("start benchmark...\n")
	timer := time.NewTimer(time.Second * time.Duration(runTime))

	go func() {
		rand.Seed(time.Now().UnixNano())
		microFreq := int(freq * 1e6)
		ticker := time.NewTicker(time.Microsecond * time.Duration(microFreq))
		for {
			select {
			case <-ticker.C:
				sendMsg(hitPercent)
			}
		}
	}()

	<-timer.C
}

func main() {
	flag.Parse()

	machines := strings.Split(*MetaEtcd, ";")
	c := etcd.NewClient(machines)
	loadSubscribers(c)
	loadDispInfo(c)
	MaxCliPerMsg = chooseMaxSubCliNum()

	if len(SubClis) == 0 {
		panic(fmt.Errorf("no subclients loaded"))
	}
	if len(DispNodes) == 0 {
		panic(fmt.Errorf("no dispatcher nodes loaded"))
	}

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM,
		syscall.SIGINT, syscall.SIGSTOP)
	go handleSignal(ch)

	dispBencher(*CliHitPercent, *RunTime, *MsgFrequency)
}
