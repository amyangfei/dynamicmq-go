package main

import (
	"encoding/binary"
	"encoding/hex"
	"fmt"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"github.com/coreos/go-etcd/etcd"
	"gopkg.in/mgo.v2/bson"
	"net"
	"strings"
)

type SubConn struct {
	subId  string
	connId string
}

type BasicMsg struct {
	cmdType uint8
	bodyLen uint16
	extra   uint8
	items   map[uint8]string
}

func binaryMsgEncode(msg *BasicMsg) []byte {
	bmsg := make([]byte, dmq.DRMsgHeaderSize)
	bmsg[0] = msg.cmdType
	binary.BigEndian.PutUint16(bmsg[1:], msg.bodyLen)
	bmsg[dmq.DRMsgCmdSize+dmq.DRMsgBodySize] = msg.extra
	var bodyLen uint16 = 0
	for itemid, item := range msg.items {
		bmsg = append(bmsg, itemid)
		bItemLen := make([]byte, dmq.DRMsgItemBodySize)
		binary.BigEndian.PutUint16(bItemLen, uint16(len(item)))
		bmsg = append(bmsg, bItemLen...)
		bmsg = append(bmsg, item...)
		bodyLen += dmq.DRMsgItemHeaderSize + uint16(len(item))
	}
	if msg.bodyLen != bodyLen {
		binary.BigEndian.PutUint16(bmsg[1:], bodyLen)
	}
	return bmsg
}

func getEtcdClient() (*etcd.Client, error) {
	var machines []string = []string{"http://localhost:4001"}
	c := etcd.NewClient(machines)
	return c, nil
}

// extract id from pattern '/sub/info/54bf9c31cc0b9a4f9f000003'
func extractSubId(etcdKey string) string {
	splitKeyArray := strings.Split(etcdKey, "/")
	return splitKeyArray[len(splitKeyArray)-1]
}

func getAllSubcli() []SubConn {
	scs := []SubConn{}
	c, _ := getEtcdClient()
	scKey := "/sub/info"
	if resp, err := c.Get(scKey, false, false); err != nil {
		panic(err)
	} else {
		if !resp.Node.Dir {
			panic(fmt.Errorf("%s should be a directory", resp.Node.Key))
		}
		for _, node := range resp.Node.Nodes {
			subId := extractSubId(node.Key)
			var connId string
			subConnKey := dmq.GetSubConnKey(subId)
			if resp, err := c.Get(subConnKey, false, false); err != nil {
				fmt.Printf("error when retrive sub connid error(%v)\n", err)
				continue
			} else {
				connId = resp.Node.Value
			}
			sc := SubConn{
				subId:  extractSubId(node.Key),
				connId: connId,
			}
			scs = append(scs, sc)
		}
	}
	return scs
}

func SendOneMsg(conn net.Conn) {
	subConns := getAllSubcli()
	sublist := make([]byte, 0)
	for i, sc := range subConns {
		if d, err := hex.DecodeString(sc.subId); err != nil {
			fmt.Printf("error format of subid %s", sc.subId)
		} else {
			sublist = append(sublist, d...)
			sublist = append(sublist, []byte(sc.connId)...)
			fmt.Println(len(sublist))
			if i != len(subConns)-1 {
				sublist = append(sublist, []byte(dmq.MDMsgSubInfoSep)...)
			}
		}
	}

	msgid := bson.NewObjectId()
	payload := "test msg payload 中文，二进制"
	msg := &BasicMsg{
		cmdType: dmq.MDMsgCmdPushMsg,
		bodyLen: 0,
		extra:   dmq.MDMsgExtraNone,
		items: map[uint8]string{
			dmq.MDMsgItemMsgidId:   string(msgid),
			dmq.MDMsgItemPayloadId: payload,
			dmq.MDMsgItemSubListId: string(sublist),
		},
	}
	bmsg := binaryMsgEncode(msg)
	conn.Write(bmsg)
}

func Connect(dst string) (net.Conn, error) {
	addr, err := net.ResolveTCPAddr("tcp", dst)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTCP("tcp", nil, addr)
	if err != nil {
		return nil, err
	}
	if err := conn.SetKeepAlive(true); err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

func main() {
	dispAddr := "localhost:6001"
	conn, err := Connect(dispAddr)
	if err != nil {
		panic(err)
	}
	SendOneMsg(conn)
}
