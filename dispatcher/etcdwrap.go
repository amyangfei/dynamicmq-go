package main

import (
	"fmt"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"github.com/coreos/go-etcd/etcd"
	"strings"
	"time"
)

func GetEtcdClient(cfg *SrvConfig) (*etcd.Client, error) {
	c := etcd.NewClient(cfg.EtcdMachiens)
	return c, nil
}

func RegisterEtcd(rmgr *RouterManager, cfg *SrvConfig) error {
	c, err := GetEtcdClient(cfg)
	if err != nil {
		return err
	}

	l := dmq.GetWaitingLockMgr(cfg.EtcdMachiens, cfg.NodeId)
	if validity, err := l.Acquire(true); err != nil {
		return err
	} else {
		// feel free to call lock release
		defer l.Release()

		start := time.Now().UnixNano()

		waitBase := dmq.GetWaitingBase(dmq.EtcdConnectorType)
		waitConns, err := c.Get(waitBase, false, false)
		if err != nil {
			return err
		}
		if len(waitConns.Node.Nodes) == 0 {
			return fmt.Errorf("failed to find waiting connector")
		}
		parsedkey := strings.Split(waitConns.Node.Nodes[0].Key, "/")
		connNodeId := parsedkey[len(parsedkey)-1]
		if len(connNodeId) != dmq.ConnectorNodeIdSize {
			return fmt.Errorf("invalid nodeid %s from etcd", connNodeId)
		}

		connInfoKey := dmq.GetInfoKey(dmq.EtcdConnectorType, connNodeId)
		resp, err := c.Get(connInfoKey, false, true)
		if err != nil {
			return err
		}
		if !resp.Node.Dir {
			return fmt.Errorf("%s should be directory", resp.Node.Key)
		}
		routeKey := fmt.Sprintf("%s/%s",
			dmq.GetInfoKey(dmq.EtcdConnectorType, connNodeId), dmq.ConnRouteAddr)
		var routeAddr string
		for _, node := range resp.Node.Nodes {
			if node.Key == routeKey {
				routeAddr = node.Value
				break
			}
		}

		// connect to connector
		if err := ConnToConnRouter(routeAddr, connNodeId, rmgr, cfg); err != nil {
			return err
		}

		// recheck lock
		expired := (time.Now().UnixNano() - start) / 1e6
		if expired >= validity {
			// TODO: destroy TCP connection with connector-router
			return fmt.Errorf("%d ms expired, lock expired", expired)
		}

		// register to etcd
		dispInfoConnIdKey := fmt.Sprintf("%s/%s",
			dmq.GetInfoKey(dmq.EtcdDispatcherType, cfg.NodeId), dmq.DispConnId)
		if _, err := c.Set(dispInfoConnIdKey, connNodeId, 0); err != nil {
			c.Delete(dispInfoConnIdKey, false)
			return err
		}

		// remove connector id from connector waiting list in etcd
		if err := dmq.UnregisterConnToWaiting(c, connNodeId); err != nil {
			// TODO: destroy TCP connection with connector-router
			c.Delete(dispInfoConnIdKey, false)
			return err
		}
	}
	return nil
}

func UnregisterEtcd(cfg *SrvConfig) error {
	c, err := GetEtcdClient(cfg)
	if err != nil {
		return err
	}

	// Here we don't re-register connector to etcd waiting list
	// and leaves this job to the connector

	infoKey := dmq.GetInfoKey(dmq.EtcdDispatcherType, cfg.NodeId)
	// delete this dispatcher information from etcd
	c.Delete(infoKey, true)

	return nil
}
