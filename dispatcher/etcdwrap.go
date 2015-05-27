package main

import (
	"fmt"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"strings"
	"time"
)

func getEtcdKeyLastKSep(key string, k int) string {
	klist := strings.Split(key, "/")
	if len(klist) < k {
		return ""
	}
	return klist[len(klist)-k]
}

func registerEtcd(rmgr *RouterManager, cfg *SrvConfig, pool *dmq.EtcdClientPool) error {
	ec, err := pool.GetEtcdClient()
	if err != nil {
		return err
	}
	defer pool.RecycleEtcdClient(ec.Id)
	c := ec.Cli

	l := dmq.GetWaitingLockMgr(c, cfg.NodeID)
	validity, err := l.Acquire(true)
	if err != nil {
		return err
	}
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
	connNodeID := parsedkey[len(parsedkey)-1]
	if len(connNodeID) != dmq.ConnectorNodeIDSize {
		return fmt.Errorf("invalid nodeid %s from etcd", connNodeID)
	}

	connInfoKey := dmq.GetInfoKey(dmq.EtcdConnectorType, connNodeID)
	resp, err := c.Get(connInfoKey, false, true)
	if err != nil {
		return err
	}
	if !resp.Node.Dir {
		return fmt.Errorf("%s should be directory", resp.Node.Key)
	}
	routeKey := fmt.Sprintf("%s/%s",
		dmq.GetInfoKey(dmq.EtcdConnectorType, connNodeID), dmq.ConnRouteAddr)
	var routeAddr string
	for _, node := range resp.Node.Nodes {
		if node.Key == routeKey {
			routeAddr = node.Value
			break
		}
	}

	// connect to connector
	if err := connToConnRouter(routeAddr, connNodeID, rmgr, cfg); err != nil {
		return err
	}

	// recheck lock
	expired := (time.Now().UnixNano() - start) / 1e6
	if expired >= validity {
		// TODO: destroy TCP connection with connector-router
		return fmt.Errorf("%d ms expired, lock expired", expired)
	}

	// register to etcd
	dispInfoBase := dmq.GetInfoKey(dmq.EtcdDispatcherType, cfg.NodeID)
	dispInfoConnIDKey := fmt.Sprintf("%s/%s", dispInfoBase, dmq.DispConnID)
	if _, err := c.Set(dispInfoConnIDKey, connNodeID, 0); err != nil {
		c.Delete(dispInfoBase, true)
		return err
	}

	dispInfoBindKey := fmt.Sprintf("%s/%s", dispInfoBase, dmq.DispBindAddr)
	tcpBind := fmt.Sprintf("%s:%d", cfg.BindIP, cfg.MatchTCPPort)
	if _, err := c.Set(dispInfoBindKey, tcpBind, 0); err != nil {
		c.Delete(dispInfoBase, true)
		return err
	}

	// remove connector id from connector waiting list in etcd
	if err := dmq.UnregisterConnToWaiting(c, connNodeID); err != nil {
		// TODO: destroy TCP connection with connector-router
		c.Delete(dispInfoConnIDKey, false)
		return err
	}

	return nil
}

func unRegisterEtcd(cfg *SrvConfig, pool *dmq.EtcdClientPool) error {
	ec, err := pool.GetEtcdClient()
	if err != nil {
		return err
	}
	defer pool.RecycleEtcdClient(ec.Id)
	c := ec.Cli

	// Here we don't re-register connector to etcd waiting list
	// and leaves this job to the connector

	infoKey := dmq.GetInfoKey(dmq.EtcdDispatcherType, cfg.NodeID)
	// delete this dispatcher information from etcd
	c.Delete(infoKey, true)

	return nil
}

func getConnRelatedDispInfo(connid string, pool *dmq.EtcdClientPool) (map[string]string, error) {
	ec, err := pool.GetEtcdClient()
	if err != nil {
		return nil, err
	}
	defer pool.RecycleEtcdClient(ec.Id)
	c := ec.Cli

	dispInfoBase := dmq.GetInfoBase(dmq.EtcdDispatcherType)
	if resp, err := c.Get(dispInfoBase, false, true); err != nil {
		return nil, err
	} else if !resp.Node.Dir {
		return nil, fmt.Errorf("%s should be a directory", resp.Node.Key)
	} else {
		for _, disp := range resp.Node.Nodes {
			if !disp.Dir {
				log.Warning("%s should be a directory", disp.Key)
				continue
			}
			addr := ""
			dispid := getEtcdKeyLastKSep(disp.Key, 1)
			found := false
			for _, v := range disp.Nodes {
				if getEtcdKeyLastKSep(v.Key, 1) == dmq.DispConnID &&
					!v.Dir && v.Value == connid {
					found = true
				}
				if getEtcdKeyLastKSep(v.Key, 1) == dmq.DispBindAddr && !v.Dir {
					addr = v.Value
				}
			}
			if found && addr != "" {
				info := map[string]string{
					"dispid": dispid,
					"addr":   addr,
				}
				return info, nil
			}
		}
	}
	return nil, fmt.Errorf("no dispatcher connected with %s", connid)
}
