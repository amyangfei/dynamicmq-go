package main

import (
	sdk "github.com/amyangfei/dynamicmq-go/sdk"
	"gopkg.in/mgo.v2/bson"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	cli, err := sdk.NewSubSdk(bson.NewObjectId().Hex())
	if err != nil {
		panic(err)
	}
	if err := cli.Auth(); err != nil {
		panic(err)
	}
	defer cli.Close()

	go cli.HeartbeatRoutine(5)
	go cli.RecvMsgRoutine()

	c := make(chan os.Signal, 1)
	signal.Notify(c, syscall.SIGHUP, syscall.SIGQUIT, syscall.SIGTERM,
		syscall.SIGINT, syscall.SIGSTOP)
	<-c
}
