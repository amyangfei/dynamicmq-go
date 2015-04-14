package main

import (
	"fmt"
	sdk "github.com/amyangfei/dynamicmq-go/sdk"
	"gopkg.in/mgo.v2/bson"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"syscall"
)

func randRange(lower, upper int) (float64, float64) {
	low := rand.Float64()*float64(upper-lower) + float64(lower)
	high := rand.Float64()*float64(upper-lower) + float64(lower)
	if low > high {
		low, high = high, low
	}
	low, _ = strconv.ParseFloat(fmt.Sprintf("%.1f", low), 64)
	high, _ = strconv.ParseFloat(fmt.Sprintf("%.1f", high), 64)
	return low, high
}

func attrGen() string {
	tpl := `{"use": 2, "low": %.1f, "high": %.1f}`
	low, high := randRange(0, 8)
	return fmt.Sprintf(tpl, low, high)
}

func subscribe(cli *sdk.SubSdk) {
	attrnames := make([]string, 0)
	attrvals := make([]string, 0)
	attrnames = append(attrnames, "strval_attr")
	attrvals = append(attrvals, `{"use": 1, "strval": "hello"}`)
	attrnames = append(attrnames, "xcoord")
	attrvals = append(attrvals, attrGen())
	attrnames = append(attrnames, "ycoord")
	attrvals = append(attrvals, attrGen())
	attrnames = append(attrnames, "zcoord")
	attrvals = append(attrvals, attrGen())
	attrnames = append(attrnames, "time")
	attrvals = append(attrvals, attrGen())

	cli.Subscribe(attrnames, attrvals)
}

func shutdown() {
}

func handleSignal(sigChan chan os.Signal) {
	for {
		s := <-sigChan
		fmt.Printf("receive a signal %s\n", s.String())
		switch s {
		case syscall.SIGQUIT, syscall.SIGTERM, syscall.SIGSTOP, syscall.SIGINT:
			shutdown()
			return
		default:
			return
		}
	}
}

func main() {
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

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM,
		syscall.SIGINT, syscall.SIGSTOP)
	handleSignal(c)
}
