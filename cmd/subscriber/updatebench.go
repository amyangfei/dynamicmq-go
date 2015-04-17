package main

import (
	"flag"
	"fmt"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	sdk "github.com/amyangfei/dynamicmq-go/sdk"
	"gopkg.in/mgo.v2/bson"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var NumClis = flag.Int("n", 4, "Number of concurrent clients")
var UpdateFrequency = flag.Float64("r", 1.0, "update frequency of per client")
var RunTime = flag.Int("t", 30, "run time in second")

func timestampVal() string {
	now := time.Now().UnixNano()
	return fmt.Sprintf("%d", now)
}

func subscribe(cli *sdk.SubSdk) {
	attrnames := make([]string, 0)
	attrvals := make([]string, 0)
	attrnames = append(attrnames, "timestamp")
	val := fmt.Sprintf("{\"use\": 1, \"%s\": \"%s\"}",
		dmq.AttrUseStr, timestampVal())
	attrvals = append(attrvals, val)

	cli.Subscribe(attrnames, attrvals)
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

func attrUpdater(cli *sdk.SubSdk, freq float64) {
	// wait random time less than 1 second
	time.Sleep(time.Millisecond * time.Duration(rand.Intn(1000)))
	microFreq := int(freq * 1e6)
	ticker := time.NewTicker(time.Microsecond * time.Duration(microFreq))
	for {
		select {
		case <-ticker.C:
			subscribe(cli)
		}
	}
}

func cliRoutine(stop chan bool, freq float64) {
	cli, err := sdk.NewSubSdk(bson.NewObjectId().Hex())
	if err != nil {
		panic(err)
	}
	if err := cli.Auth(); err != nil {
		panic(err)
	}
	defer cli.Close()

	subscribe(cli)

	go cli.HeartbeatRoutine(300)
	go cli.RecvMsgRoutine()
	go attrUpdater(cli, freq)

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

func attrUpdateTester(numClis int, freq float64, runTime int) {
	fmt.Printf("start benchmark...\n")
	timer := time.NewTimer(time.Second * time.Duration(runTime))

	stops := make([]chan bool, 0)
	for i := 0; i < numClis; i++ {
		stops = append(stops, make(chan bool))
	}

	for i := 0; i < numClis; i++ {
		go cliRoutine(stops[i], freq)
	}

	<-timer.C
	for _, stop := range stops {
		stop <- true
	}

}

func main() {
	flag.Parse()

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM,
		syscall.SIGINT, syscall.SIGSTOP)
	go handleSignal(c)

	attrUpdateTester(*NumClis, *UpdateFrequency, *RunTime)
}
