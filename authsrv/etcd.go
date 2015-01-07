package main

import (
	"fmt"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"github.com/coreos/go-etcd/etcd"
	"math/rand"
	"time"
)

func getEtcdClient(machines []string) (*etcd.Client, error) {
	c := etcd.NewClient(machines)
	return c, nil
}

func GetConnector(machines []string) (string, error) {
	rand.Seed(time.Now().UnixNano())
	c, err := getEtcdClient(machines)
	if err != nil {
		return "", err
	}
	infoKey := dmq.GetInfoBase(dmq.EtcdConnectorType)
	resp, err := c.Get(infoKey, false, true)
	if err != nil {
		return "", err
	}
	if !resp.Node.Dir {
		return "", fmt.Errorf("%s is not a directory", resp.Node.Key)
	}
	connNum := len(resp.Node.Nodes)
	if connNum > 0 {
		idx := rand.Intn(connNum)
		connInfo := resp.Node.Nodes[idx]
		if !connInfo.Dir {
			return "", fmt.Errorf("%s is not a directory", connInfo.Key)
		}
		subAddrKey := fmt.Sprintf("%s/%s", connInfo.Key, dmq.ConnSubAddr)
		for _, info := range connInfo.Nodes {
			if info.Key == subAddrKey {
				return info.Value, nil
			}
		}
	}
	return "", fmt.Errorf("connector not found")
}
