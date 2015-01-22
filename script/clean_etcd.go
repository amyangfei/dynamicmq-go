package main

import (
	"flag"
	"fmt"
	"github.com/coreos/go-etcd/etcd"
	"strings"
)

func GetEtcdClient(machines []string) *etcd.Client {
	return etcd.NewClient(machines)
}

func extractEtcdLastKey(key string) string {
	s := strings.Split(key, "/")
	return s[len(s)-1]
}

func stringIndex(target string, source []string) int {
	for i, val := range source {
		if target == val {
			return i
		}
	}
	return -1
}

func getAliveConnector(machines []string) ([]string, error) {
	c := GetEtcdClient(machines)
	connKey := "/conn/info"
	if resp, err := c.Get(connKey, false, false); err != nil {
		return nil, err
	} else {
		if !resp.Node.Dir {
			return nil, fmt.Errorf("%s node should be dir", connKey)
		}
		connIds := make([]string, 0)
		for _, node := range resp.Node.Nodes {
			connIds = append(connIds, extractEtcdLastKey(node.Key))
		}
		return connIds, nil
	}
}

func CleanSubscriber(machines []string) error {
	fmt.Println("start cleaning disp in etcd...")

	toCleanSubIds := make([]string, 0)

	c := GetEtcdClient(machines)

	connids, err := getAliveConnector(machines)
	if err != nil {
		return err
	}
	subKey := "/sub/info"
	resp, err := c.Get(subKey, false, true)
	if err != nil {
		return err
	}
	for _, node := range resp.Node.Nodes {
		subId := extractEtcdLastKey(node.Key)
		subConnKey := fmt.Sprintf("/sub/info/%s/conn_id", subId)
		if connResp, err := c.Get(subConnKey, false, false); err != nil {
			fmt.Printf("retrive %s error(%v)\n", subConnKey, err)
		} else {
			connId := extractEtcdLastKey(connResp.Node.Value)
			if stringIndex(connId, connids) == -1 {
				// case1: if a subscriber's corresponding connector is dead,
				// remove this subscriber
				toCleanSubIds = append(toCleanSubIds, subId)
			} else {
				connSubKey := fmt.Sprintf("/conn/info/%s/sub/%s", connId, subId)
				if _, err := c.Get(connSubKey, false, false); err != nil {
					// case2: a subscriber's corresponding connector is alive,
					// however it isn't connecting with this connector
					toCleanSubIds = append(toCleanSubIds, subId)
				}
			}
		}
	}

	// clean work
	for _, subId := range toCleanSubIds {
		fmt.Printf("cleaning sub %s...\n", subId)
		subKey := fmt.Sprintf("/sub/info/%s", subId)
		c.Delete(subKey, true)
	}

	fmt.Println("clean disp end...")
	return nil
}

func main() {
	etcdm := flag.String("etcd", "http://localhost:4001", "etcd machines, separated by ','")
	flag.Parse()

	machines := strings.Split(*etcdm, ",")
	if err := CleanSubscriber(machines); err != nil {
		fmt.Println("clean subscriber with error(%v)", err)
	}
}
