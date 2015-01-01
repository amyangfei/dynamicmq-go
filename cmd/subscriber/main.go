package main

import (
	sdk "github.com/amyangfei/dynamicmq-go/sdk"
	"time"
)

func main() {
	cli := sdk.SubSdk{Token: "testtoken"}
	// cli := sdk.NewSubSdk()
	cli.Connect("localhost:7253")
	for i := 0; i < 10; i++ {
		cli.Heartbeat()
		time.Sleep(time.Second * 1)
	}
	cli.Close()
}
