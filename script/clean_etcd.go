package main

import (
	"flag"
	"fmt"
	"github.com/coreos/go-etcd/etcd"
	"strings"
)

func getEtcdClient(machines []string) *etcd.Client {
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
	c := getEtcdClient(machines)
	connKey := "/conn/info"
	resp, err := c.Get(connKey, false, false)
	if err != nil {
		return nil, err
	}
	if !resp.Node.Dir {
		return nil, fmt.Errorf("%s node should be dir", connKey)
	}
	var connIDs []string
	for _, node := range resp.Node.Nodes {
		connIDs = append(connIDs, extractEtcdLastKey(node.Key))
	}
	return connIDs, nil
}

func cleanSubscriber(machines []string) error {
	fmt.Println("start cleaning disp in etcd...")

	var toCleanSubIds []string

	c := getEtcdClient(machines)

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
		subID := extractEtcdLastKey(node.Key)
		subConnKey := fmt.Sprintf("/sub/info/%s/conn_id", subID)
		if connResp, err := c.Get(subConnKey, false, false); err != nil {
			fmt.Printf("retrive %s error(%v)\n", subConnKey, err)
		} else {
			connID := extractEtcdLastKey(connResp.Node.Value)
			if stringIndex(connID, connids) == -1 {
				// case1: if a subscriber's corresponding connector is dead,
				// remove this subscriber
				toCleanSubIds = append(toCleanSubIds, subID)
			} else {
				connSubKey := fmt.Sprintf("/conn/info/%s/sub/%s", connID, subID)
				if _, err := c.Get(connSubKey, false, false); err != nil {
					// case2: a subscriber's corresponding connector is alive,
					// however it isn't connecting with this connector
					toCleanSubIds = append(toCleanSubIds, subID)
				}
			}
		}
	}

	// clean work
	for _, subID := range toCleanSubIds {
		fmt.Printf("cleaning sub %s...\n", subID)
		subKey := fmt.Sprintf("/sub/info/%s", subID)
		c.Delete(subKey, true)
	}

	fmt.Println("clean disp end...")
	return nil
}

func main() {
	etcdm := flag.String("etcd", "http://localhost:4001", "etcd machines, separated by ','")
	flag.Parse()

	machines := strings.Split(*etcdm, ",")
	if err := cleanSubscriber(machines); err != nil {
		fmt.Println("clean subscriber with error(%v)", err)
	}
}
