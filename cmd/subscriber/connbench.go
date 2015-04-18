package main

import (
	"flag"
	"fmt"
	sdk "github.com/amyangfei/dynamicmq-go/sdk"
	"gopkg.in/mgo.v2/bson"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

var NumClis = flag.Int("n", 4, "Number of concurrent clients")
var HbInterval = flag.Int("b", 60, "heartbeat interval in second")
var UpdateFrequency = flag.Float64("r", 5.0, "msg frequency of per client")
var RunTime = flag.Int("t", 30, "run time in second")
var IpList = flag.String("i", "127.0.0.1", "bind ip list for sdk, seperated by ';'")
var PortRange = flag.String("p", "10000;60000", "bind port range for sdk e.g. 10000;60000")
var PreAuth = flag.String("a", "127.0.0.1:9000", "address of auth server")
var Addrs []string
var AddrIdx int

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

func sendmsg(cli *sdk.SubSdk) {
}

func msgSendRoutine(cli *sdk.SubSdk, freq float64) {
	// wait random time less than 1 second
	time.Sleep(time.Millisecond * time.Duration(rand.Intn(1000)))
	microFreq := int(freq * 1e6)
	ticker := time.NewTicker(time.Microsecond * time.Duration(microFreq))
	for {
		select {
		case <-ticker.C:
			sendmsg(cli)
		}
	}
}

func createSubSdk() (*sdk.SubSdk, error) {
	var cli *sdk.SubSdk = nil
	for cli == nil {
		var err error
		if cli, err = sdk.NewSubSdk(bson.NewObjectId().Hex(), *PreAuth); err != nil {
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

func cliRoutine(stop chan bool, freq float64, hbInterval int) {
	cli, err := createSubSdk()
	if err != nil {
		panic(err)
	}

	go cli.HeartbeatRoutine(hbInterval)
	go cli.RecvMsgRoutine()
	// go msgSendRoutine(cli, freq)

	for {
		select {
		case <-time.After(time.Second * 5):
			continue
		case <-stop:
			cli.Close()
			return
		}
	}
}

func connBencher(numClis, hbInterval int, freq float64, runTime int) {
	fmt.Printf("start benchmark...\n")
	timer := time.NewTimer(time.Second * time.Duration(runTime))

	stops := make([]chan bool, 0)
	for i := 0; i < numClis; i++ {
		stops = append(stops, make(chan bool))
	}

	startInterval := hbInterval * 1e6 / numClis
	for i := 0; i < numClis; i++ {
		go cliRoutine(stops[i], freq, hbInterval)
		time.Sleep(time.Microsecond * time.Duration(startInterval))
	}

	<-timer.C
	for _, stop := range stops {
		stop <- true
	}

}

func main() {
	flag.Parse()
	prepareAddrs(*IpList, *PortRange)

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM,
		syscall.SIGINT, syscall.SIGSTOP)
	go handleSignal(c)

	connBencher(*NumClis, *HbInterval, *UpdateFrequency, *RunTime)
}
