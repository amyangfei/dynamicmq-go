package main

import (
	sdk "github.com/amyangfei/dynamicmq-go/sdk"
)

func main() {
	cli := sdk.SubSdk{}
	cli.Connect("localhost:7253")
	for i := 0; i < 100000; i++ {
		cli.Heartbeat()
	}
	cli.Close()
}
