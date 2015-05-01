package main

import (
	"flag"
	"fmt"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	sdk "github.com/amyangfei/dynamicmq-go/sdk"
	"github.com/coreos/go-etcd/etcd"
	"gopkg.in/mgo.v2/bson"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

var ClientNum = flag.Int("n", 10, "number of subscribe client")
var RunTime = flag.Int("t", 60, "run time in second")
var MetaEtcd = flag.String("e", "http://127.0.0.1:4001", "address of meta-storage")
var IpList = flag.String("i", "127.0.0.1", "bind ip list for sdk, seperated by ';'")
var PortRange = flag.String("P", "10000;60000", "bind port range for sdk e.g. 10000;60000")
var AuthAddr = flag.String("a", "127.0.0.1:9000", "address of auth server")
var RangePCT = flag.Int("p", 20, "percent of each attribute dimension")
var Addrs []string
var AddrIdx int

var IdxBase *IndexBase

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

func prepareAddrs(ipList, portRange string) {
	ips := strings.Split(ipList, ";")
	ports := strings.Split(portRange, ";")
	pstart, err := strconv.Atoi(ports[0])
	if err != nil {
		panic(err)
	}
	pend, err := strconv.Atoi(ports[1])
	if err != nil {
		panic(err)
	}
	for _, ip := range ips {
		for i := pstart; i <= pend; i++ {
			Addrs = append(Addrs, fmt.Sprintf("%s:%d", ip, i))
		}
	}
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

func attrGen(min, max int) string {
	tpl := `{"use": 2, "low": %.1f, "high": %.1f}`
	low := rand.Float64()*float64(max-min)*((100.0-float64(*RangePCT))/100.0) + float64(min)
	high := low + float64(max-min)*float64(*RangePCT)/100.0
	return fmt.Sprintf(tpl, low, high)
}

func subscribe(cli *sdk.SubSdk) {
	attrnames := make([]string, 0)
	attrvals := make([]string, 0)
	for name, ib := range IdxBase.attrbases {
		if ib.use == dmq.AttrUseField[dmq.AttrUseRange] {
			attrnames = append(attrnames, name)
			attrvals = append(attrvals, attrGen(ib.low, ib.high))
		}
	}

	cli.Subscribe(attrnames, attrvals)
}

func createSubSdk() (*sdk.SubSdk, error) {
	var cli *sdk.SubSdk = nil
	for cli == nil {
		var err error
		if cli, err = sdk.NewSubSdk(bson.NewObjectId().Hex(), *AuthAddr); err != nil {
			cli = nil
			continue
		}
		if AddrIdx > len(Addrs) {
			return nil, fmt.Errorf("ip pool exhausted")
		}
		cli.SetLaddr(Addrs[AddrIdx])
		AddrIdx++
		if err := cli.Auth(); err != nil {
			cli.Close()
			cli = nil
		}
	}
	return cli, nil
}

func cliRoutine(authaddr string) {
	cli, err := createSubSdk()
	if err != nil {
		panic(err)
	}

	subscribe(cli)

	go cli.HeartbeatRoutine(300)
	go cli.RecvMsgRoutine()
	// go attrUpdater(cli, freq)
}

func matchBencher(cliNum, runTime int, authaddr string) {
	fmt.Printf("start benchmark...\n")
	timer := time.NewTimer(time.Second * time.Duration(runTime))

	for i := 0; i < cliNum; i++ {
		cliRoutine(authaddr)
	}

	<-timer.C
}

func main() {
	flag.Parse()

	prepareAddrs(*IpList, *PortRange)

	machines := strings.Split(*MetaEtcd, ";")
	c := etcd.NewClient(machines)
	IdxBase = &IndexBase{}
	if err := LoadIndexBase(c, IdxBase); err != nil {
		panic(err)
	}

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM,
		syscall.SIGINT, syscall.SIGSTOP)
	go handleSignal(ch)

	matchBencher(*ClientNum, *RunTime, *AuthAddr)
}
