package main

import (
	sdk "github.com/amyangfei/dynamicmq-go/sdk"
	"gopkg.in/mgo.v2/bson"
)

func main() {
	cli, err := sdk.NewSubSdk(bson.NewObjectId().Hex())
	if err != nil {
		panic(err)
	}
	if err := cli.Auth(); err != nil {
		panic(err)
	}

	for i := 0; i < 100000; i++ {
		cli.Heartbeat()
	}
	cli.Close()
}
