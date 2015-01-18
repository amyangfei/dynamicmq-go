package main

import (
	"fmt"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"github.com/coreos/go-etcd/etcd"
	"strings"
)

func GetEtcdClient(cfg *SrvConfig) (*etcd.Client, error) {
	// TODO: client buffer
	c := etcd.NewClient(cfg.EtcdMachiens)
	return c, nil
}

func RegisterEtcd(cfg *SrvConfig) error {
	c, err := GetEtcdClient(cfg)
	if err != nil {
		return err
	}

	// register single connector information
	subs := strings.Split(cfg.SubTCPBind, ":")
	subPort := subs[len(subs)-1]
	routes := strings.Split(cfg.RouterTCPBind, ":")
	routePort := routes[len(routes)-1]
	info := map[string]string{
		dmq.ConnSubAddr:   fmt.Sprintf("%s:%s", cfg.BindIp, subPort),
		dmq.ConnRouteAddr: fmt.Sprintf("%s:%s", cfg.BindIp, routePort),
		dmq.ConnCapacity:  fmt.Sprintf("%d", cfg.Capacity),
		dmq.ConnLoad:      "0",
		dmq.ConnStatus:    "new",
	}
	baseKey := dmq.GetInfoKey(dmq.EtcdConnectorType, cfg.NodeId)
	for k, v := range info {
		_, err := c.Set(fmt.Sprintf("%s/%s", baseKey, k), v, 0)
		if err != nil {
			c.Delete(baseKey, true)
			return err
		}
	}

	if err := RegisterWaiting(c, cfg); err != nil {
		c.Delete(baseKey, true)
		return err
	}

	return nil
}

func UnregisterEtcd(cfg *SrvConfig) error {
	c, err := GetEtcdClient(cfg)
	if err != nil {
		return err
	}

	infoKey := dmq.GetInfoKey(dmq.EtcdConnectorType, cfg.NodeId)
	c.Delete(infoKey, true)

	if err := UnregisterWaiting(c, cfg); err != nil {
		return err
	}

	return nil
}

func RegisterWaiting(c *etcd.Client, cfg *SrvConfig) error {
	if c == nil {
		var err error
		c, err = GetEtcdClient(cfg)
		if err != nil {
			return err
		}
	}
	l := dmq.GetWaitingLockMgr(cfg.EtcdMachiens, cfg.NodeId)
	_, err := l.Acquire(true)
	defer l.Release()
	if err != nil {
		return err
	} else {
		// register connector to etcd connector waiting list
		if err := dmq.RegisterConnToWaiting(c, cfg.NodeId); err != nil {
			return err
		}
	}
	return nil
}

func UnregisterWaiting(c *etcd.Client, cfg *SrvConfig) error {
	if c == nil {
		var err error
		c, err = GetEtcdClient(cfg)
		if err != nil {
			return err
		}
	}
	l := dmq.GetWaitingLockMgr(cfg.EtcdMachiens, cfg.NodeId)
	_, err := l.Acquire(true)
	defer l.Release()
	if err != nil {
		return err
	} else {
		// unregister connector from waiting list
		dmq.UnregisterConnToWaiting(c, cfg.NodeId)
	}
	return nil
}

func GetConnInfo(cfg *SrvConfig) (*etcd.Response, error) {
	infoKey := dmq.GetInfoKey(dmq.EtcdConnectorType, cfg.NodeId)
	c, err := GetEtcdClient(cfg)
	if err != nil {
		return nil, err
	}
	return c.Get(infoKey, false, true)
}
