package main

import (
	"fmt"
	sdk "github.com/amyangfei/dynamicmq-go/sdk"
	"gopkg.in/mgo.v2/bson"
	"os"
	"os/signal"
	"syscall"
)

func subscribe(cli *sdk.SubSdk) {
	attrnames := make([]string, 0)
	attrvals := make([]string, 0)
	attrnames = append(attrnames, "strval_attr")
	attrvals = append(attrvals, `{"use": 1, "strval": "hello"}`)
	attrnames = append(attrnames, "xcoord")
	attrvals = append(attrvals, `{"use": 2, "low": 2.5, "high": 4.7}`)
	attrnames = append(attrnames, "ycoord")
	attrvals = append(attrvals, `{"use": 2, "low": 0.4, "high": 1.2}`)
	attrnames = append(attrnames, "zcoord")
	attrvals = append(attrvals, `{"use": 2, "low": 7, "high": 9.9}`)
	attrnames = append(attrnames, "time")
	attrvals = append(attrvals, `{"use": 2, "low": 3, "high": 6.2}`)

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
