package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"github.com/coreos/go-etcd/etcd"
	"math/rand"
	"net"
	"strconv"
	"strings"
)

type IndexBase struct {
	dimension int
	// mapping from attribute name to this attribute's information(AttrBase)
	attrbases map[string]*AttrBase
}

type AttrBase struct {
	name      string
	use       int
	low, high int
	sigval    []string
}

type BasicMsg struct {
	cmdType uint8
	bodyLen uint16
	extra   uint8
	items   map[uint8]string
}

func binaryMsgEncode(msg *BasicMsg) []byte {
	bmsg := make([]byte, dmq.PMMsgHeaderSize)
	bmsg[0] = msg.cmdType
	binary.BigEndian.PutUint16(bmsg[1:], msg.bodyLen)
	bmsg[dmq.PMMsgCmdSize+dmq.PMMsgBodySize] = msg.extra
	var bodyLen uint16 = 0
	for itemid, item := range msg.items {
		bmsg = append(bmsg, itemid)
		bItemLen := make([]byte, dmq.PMMsgItemBodySize)
		binary.BigEndian.PutUint16(bItemLen, uint16(len(item)))
		bmsg = append(bmsg, bItemLen...)
		bmsg = append(bmsg, item...)
		bodyLen += dmq.PMMsgItemHeaderSize + uint16(len(item))
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

func LoadIndexBase(c *etcd.Client, idxBase *IndexBase) error {
	if idxBase.attrbases == nil {
		idxBase.attrbases = make(map[string]*AttrBase)
	}

	// load /idx/info/dimension
	dimKey := dmq.GetIndexBaseDim()
	if resp, err := c.Get(dimKey, false, false); err != nil {
		return err
	} else {
		dimension, err := strconv.Atoi(resp.Node.Value)
		if err != nil {
			return err
		}
		idxBase.dimension = dimension
	}

	idxBaseBound := dmq.GetIndexBaseBound()
	if resp, err := c.Get(idxBaseBound, false, true); err != nil {
		return err
	} else if !resp.Node.Dir {
		return fmt.Errorf("%v should be a directory", resp.Node.Key)
	} else {
		// iteration for /idx/info/bound
		for _, attrNameNode := range resp.Node.Nodes {
			if !attrNameNode.Dir {
				return fmt.Errorf("%v should be a directory", attrNameNode.Key)
			}

			lstr, ustr := "", ""
			keySp := strings.Split(attrNameNode.Key, "/")
			attrName := keySp[len(keySp)-1]
			lowerKey := dmq.GetIndexBaseBoundKey(attrName, dmq.IdxAttrLower)
			upperKey := dmq.GetIndexBaseBoundKey(attrName, dmq.IdxAttrUpper)
			// iteration /idx/info/<attr-name> for lower and upper bound
			for _, attrNode := range attrNameNode.Nodes {
				if attrNode.Key == lowerKey {
					if attrNode.Dir {
						return fmt.Errorf("%v should be a directory", attrNode.Key)
					}
					lstr = attrNode.Value
				}
				if attrNode.Key == upperKey {
					if attrNode.Dir {
						return fmt.Errorf("%v should be a directory", attrNode.Key)
					}
					ustr = attrNode.Value
				}
			}

			lower, err := strconv.Atoi(lstr)
			if err != nil {
				return fmt.Errorf("invalid lower bound '%s' for %s", lstr, attrNameNode.Key)
			}
			upper, err := strconv.Atoi(ustr)
			if err != nil {
				return fmt.Errorf("invalid upper bound '%s' for %s", ustr, attrNameNode.Key)
			}

			// update indxbase
			attrbase := &AttrBase{
				name: attrName,
				use:  dmq.AttrUseField["range"],
				low:  lower,
				high: upper,
			}
			idxBase.attrbases[attrName] = attrbase
		}
	}

	return nil
}

func SendOneMsg(conn net.Conn, idxbase *IndexBase) {
	attr := make(map[string]float64)
	for name, ab := range idxbase.attrbases {
		if ab.use == dmq.AttrUseField["range"] {
			rf := rand.Float64()*float64(ab.high-ab.low) + float64(ab.low)
			attr[name], _ = strconv.ParseFloat(fmt.Sprintf("%.1f", rf), 64)
		}
	}
	battr, err := json.Marshal(attr)
	if err != nil {
		panic(err)
	}

	payload := "test publish message 你好"
	msg := &BasicMsg{
		cmdType: dmq.PMMsgCmdPushMsg,
		bodyLen: 0,
		extra:   dmq.PMMsgExtraNone,
		items: map[uint8]string{
			dmq.PMMsgItemAttributeID: string(battr),
			dmq.PMMsgItemPayloadID:   payload,
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
	return conn, nil
}

func main() {
	pubAddr := "localhost:8000"

	IdxBase := &IndexBase{}
	c, _ := getEtcdClient()
	if err := LoadIndexBase(c, IdxBase); err != nil {
		panic(err)
	}

	conn, err := Connect(pubAddr)
	if err != nil {
		panic(err)
	}
	SendOneMsg(conn, IdxBase)
}
