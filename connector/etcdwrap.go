package main

import (
	"fmt"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"github.com/coreos/go-etcd/etcd"
	"strings"
)

func registerEtcd(cfg *SrvConfig, pool *dmq.EtcdClientPool) error {
	ec, err := pool.GetEtcdClient()
	if err != nil {
		return err
	}
	defer pool.RecycleEtcdClient(ec.Id)
	c := ec.Cli

	// register single connector information
	subs := strings.Split(cfg.SubTCPBind, ":")
	subPort := subs[len(subs)-1]
	routes := strings.Split(cfg.RouterTCPBind, ":")
	routePort := routes[len(routes)-1]
	info := map[string]string{
		dmq.ConnSubAddr:   fmt.Sprintf("%s:%s", cfg.BindIP, subPort),
		dmq.ConnRouteAddr: fmt.Sprintf("%s:%s", cfg.BindIP, routePort),
		dmq.ConnCapacity:  fmt.Sprintf("%d", cfg.Capacity),
		dmq.ConnLoad:      "0",
		dmq.ConnStatus:    "new",
	}
	baseKey := dmq.GetInfoKey(dmq.EtcdConnectorType, cfg.NodeID)
	for k, v := range info {
		_, err := c.Set(fmt.Sprintf("%s/%s", baseKey, k), v, 0)
		if err != nil {
			c.Delete(baseKey, true)
			return err
		}
	}

	if err := registerWaiting(cfg, pool); err != nil {
		c.Delete(baseKey, true)
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

	infoKey := dmq.GetInfoKey(dmq.EtcdConnectorType, cfg.NodeID)
	c.Delete(infoKey, true)

	if err := unRegisterWaiting(cfg, pool); err != nil {
		return err
	}

	return nil
}

func registerWaiting(cfg *SrvConfig, pool *dmq.EtcdClientPool) error {
	ec, err := pool.GetEtcdClient()
	if err != nil {
		return err
	}
	defer pool.RecycleEtcdClient(ec.Id)
	c := ec.Cli

	l := dmq.GetWaitingLockMgr(c, cfg.NodeID)
	_, err = l.Acquire(true)
	defer l.Release()
	if err != nil {
		return err
	}
	// register connector to etcd connector waiting list
	if err := dmq.RegisterConnToWaiting(c, cfg.NodeID); err != nil {
		return err
	}

	return nil
}

func unRegisterWaiting(cfg *SrvConfig, pool *dmq.EtcdClientPool) error {
	ec, err := pool.GetEtcdClient()
	if err != nil {
		return err
	}
	defer pool.RecycleEtcdClient(ec.Id)
	c := ec.Cli

	l := dmq.GetWaitingLockMgr(c, cfg.NodeID)
	_, err = l.Acquire(true)
	defer l.Release()
	if err != nil {
		return err
	}
	// unregister connector from waiting list
	dmq.UnregisterConnToWaiting(c, cfg.NodeID)

	return nil
}

func getConnInfo(cfg *SrvConfig, pool *dmq.EtcdClientPool) (*etcd.Response, error) {
	infoKey := dmq.GetInfoKey(dmq.EtcdConnectorType, cfg.NodeID)

	ec, err := pool.GetEtcdClient()
	if err != nil {
		return nil, err
	}
	defer pool.RecycleEtcdClient(ec.Id)
	c := ec.Cli

	return c.Get(infoKey, false, true)
}

/*
func RegisterSub(cli *SubClient, cfg *SrvConfig, pool *dmq.EtcdClientPool) error {
	ec, err := pool.GetEtcdClient()
	if err != nil {
		return err
	}
	defer pool.RecycleEtcdClient(ec.Id)
	c := ec.Cli

	subConnKey := dmq.GetSubConnKey(cli.id.Hex())
	if _, err := c.Set(subConnKey, cfg.NodeID, 0); err != nil {
		return err
	}
	return nil
}

// UpdateSubAttr succeeds only if the given key does not yet exists.
func createSubAttr(cli *SubClient, attr *Attribute, cfg *SrvConfig, pool *dmq.EtcdClientPool) error {
	key := dmq.getSubAttrKey(cli.id.Hex(), attr.name)
	jsonStr, err := AttrMarshal(attr)
	if err != nil {
		return err
	}

	ec, err := pool.GetEtcdClient()
	if err != nil {
		return err
	}
	defer pool.RecycleEtcdClient(ec.Id)
	c := ec.Cli

	_, err = c.Create(key, string(jsonStr), 0)
	return err
}

// UpdateSubAttr succeeds only if the given key already exists.
func UpdateSubAttr(cli *SubClient, attr *Attribute, cfg *SrvConfig, pool *dmq.EtcdClientPool) error {
	key := dmq.getSubAttrKey(cli.id.Hex(), attr.name)
	jsonStr, err := AttrMarshal(attr)
	if err != nil {
		return err
	}

	ec, err := pool.GetEtcdClient()
	if err != nil {
		return err
	}
	defer pool.RecycleEtcdClient(ec.Id)
	c := ec.Cli

	_, err = c.Update(key, string(jsonStr), 0)
	return err
}

func getSubAttr(cli *SubClient, attrname string, cfg *SrvConfig, pool *dmq.EtcdClientPool) (string, error) {
	key := dmq.getSubAttrKey(cli.id.Hex(), attrname)

	ec, err := pool.GetEtcdClient()
	if err != nil {
		return "", err
	}
	defer pool.RecycleEtcdClient(ec.Id)
	c := ec.Cli

	if resp, err := c.Get(key, false, false); err != nil {
		return "", err
	} else {
		return resp.Node.Value, nil
	}
}

func removeSubAttr(cli *SubClient, attrname string, cfg *SrvConfig, pool *dmq.EtcdClientPool) error {
	key := dmq.getSubAttrKey(cli.id.Hex(), attrname)

	ec, err := pool.GetEtcdClient()
	if err != nil {
		return err
	}
	defer pool.RecycleEtcdClient(ec.Id)
	c := ec.Cli

	if _, err := c.Delete(key, true); err != nil {
		return err
	}
	return nil
}

func RemoveSub(cli *SubClient, cfg *SrvConfig, pool, attrPool *dmq.EtcdClientPool) error {
	attrEc, err := attrPool.GetEtcdClient()
	if err != nil {
		return err
	}
	defer attrPool.RecycleEtcdClient(attrEc.ID)
	attrC := attrEc.Cli

	attrKey := dmq.getSubAttrCliBase(cli.id.Hex())

	if _, err := attrC.Delete(attrKey, true); err != nil {
		return err
	}

	return nil
}
*/
