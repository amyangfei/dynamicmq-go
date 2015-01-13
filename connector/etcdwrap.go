package main

import (
	"fmt"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"github.com/coreos/go-etcd/etcd"
	"strings"
)

func GetEtcdClient(cfg *SrvConfig) (*etcd.Client, error) {
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

	// register to connector waiting list
	if err := dmq.RegisterConnToWaiting(c, cfg.NodeId); err != nil {
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

	// unregister connector from waiting list
	dmq.UnregisterConnToWaiting(c, cfg.NodeId)

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
