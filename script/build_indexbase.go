package main

import (
	"flag"
	"fmt"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"github.com/coreos/go-etcd/etcd"
)

var etcdMachines = []string{"http://localhost:4001"}

var dimension = 4
var names = []string{"xcoord", "ycoord", "zcoord", "time"}
var lower = []int{0, 0, 0, 0}
var upper = []int{8, 8, 8, 8}

var cmd = flag.String("cmd", "build", "build/clean")

func buildIndexbase() {
	c := etcd.NewClient(etcdMachines)
	if err := dmq.NewAttrIndexBase(c, dimension, names, lower, upper); err != nil {
		fmt.Printf("build index base with error: %v", err)
	} else {
		fmt.Printf("build index base successfully...\n")
	}
}

func cleanIndexbase() {
	c := etcd.NewClient(etcdMachines)
	if err := dmq.RemoveAttrIndexBase(c); err != nil {
		fmt.Printf("clean index base with error: %v", err)
	} else {
		fmt.Printf("clean index base successfully...\n")
	}
}

func main() {
	flag.Parse()

	if *cmd == "build" {
		buildIndexbase()
	} else if *cmd == "clean" {
		cleanIndexbase()
	} else {
		fmt.Printf("unknown command %s\n", *cmd)
	}
}
