package main

import (
	"fmt"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"github.com/coreos/go-etcd/etcd"
	"strings"
)

func RegisterEtcd(cfg *SrvConfig, pool *dmq.EtcdClientPool) error {
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

	if err := RegisterWaiting(cfg, pool); err != nil {
		c.Delete(baseKey, true)
		return err
	}

	return nil
}

func UnregisterEtcd(cfg *SrvConfig, pool *dmq.EtcdClientPool) error {
	ec, err := pool.GetEtcdClient()
	if err != nil {
		return err
	}
	defer pool.RecycleEtcdClient(ec.Id)
	c := ec.Cli

	infoKey := dmq.GetInfoKey(dmq.EtcdConnectorType, cfg.NodeId)
	c.Delete(infoKey, true)

	if err := UnregisterWaiting(cfg, pool); err != nil {
		return err
	}

	return nil
}

func RegisterWaiting(cfg *SrvConfig, pool *dmq.EtcdClientPool) error {
	ec, err := pool.GetEtcdClient()
	if err != nil {
		return err
	}
	defer pool.RecycleEtcdClient(ec.Id)
	c := ec.Cli

	l := dmq.GetWaitingLockMgr(c, cfg.NodeId)
	_, err = l.Acquire(true)
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

func UnregisterWaiting(cfg *SrvConfig, pool *dmq.EtcdClientPool) error {
	ec, err := pool.GetEtcdClient()
	if err != nil {
		return err
	}
	defer pool.RecycleEtcdClient(ec.Id)
	c := ec.Cli

	l := dmq.GetWaitingLockMgr(c, cfg.NodeId)
	_, err = l.Acquire(true)
	defer l.Release()
	if err != nil {
		return err
	} else {
		// unregister connector from waiting list
		dmq.UnregisterConnToWaiting(c, cfg.NodeId)
	}
	return nil
}

func GetConnInfo(cfg *SrvConfig, pool *dmq.EtcdClientPool) (*etcd.Response, error) {
	infoKey := dmq.GetInfoKey(dmq.EtcdConnectorType, cfg.NodeId)

	ec, err := pool.GetEtcdClient()
	if err != nil {
		return nil, err
	}
	defer pool.RecycleEtcdClient(ec.Id)
	c := ec.Cli

	return c.Get(infoKey, false, true)
}

func RegisterSub(cli *SubClient, cfg *SrvConfig, pool *dmq.EtcdClientPool) error {
	ec, err := pool.GetEtcdClient()
	if err != nil {
		return err
	}
	defer pool.RecycleEtcdClient(ec.Id)
	c := ec.Cli

	subConnKey := dmq.GetSubConnKey(cli.id.Hex())
	if _, err := c.Set(subConnKey, cfg.NodeId, 0); err != nil {
		return err
	}
	connSubKey := dmq.GetConnSubKey(cfg.NodeId, cli.id.Hex())
	if _, err := c.SetDir(connSubKey, 0); err != nil {
		return err
	}
	return nil
}

// UpdateSubAttr succeeds only if the given key does not yet exists.
func CreateSubAttr(cli *SubClient, attr *Attribute, cfg *SrvConfig, pool *dmq.EtcdClientPool) error {
	key := dmq.GetSubAttrKey(cli.id.Hex(), attr.name)
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
	key := dmq.GetSubAttrKey(cli.id.Hex(), attr.name)
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

func GetSubAttr(cli *SubClient, attrname string, cfg *SrvConfig, pool *dmq.EtcdClientPool) (string, error) {
	key := dmq.GetSubAttrKey(cli.id.Hex(), attrname)

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

func RemoveSubAttr(cli *SubClient, attrname string, cfg *SrvConfig, pool *dmq.EtcdClientPool) error {
	key := dmq.GetSubAttrKey(cli.id.Hex(), attrname)

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

func RemoveSub(cli *SubClient, cfg *SrvConfig, pool *dmq.EtcdClientPool) error {
	ec, err := pool.GetEtcdClient()
	if err != nil {
		return err
	}
	defer pool.RecycleEtcdClient(ec.Id)
	c := ec.Cli

	key := dmq.GetInfoKey(dmq.EtcdSubscriberType, cli.id.Hex())
	attrKey := dmq.GetSubAttrCliBase(cli.id.Hex())
	connSubKey := dmq.GetConnSubKey(cfg.NodeId, cli.id.Hex())

	_, infoErr := c.Delete(key, true)
	_, attrErr := c.Delete(attrKey, true)
	_, subErr := c.Delete(connSubKey, true)

	err = nil
	if infoErr != nil {
		err = fmt.Errorf("remove subinfo err: %v", infoErr)
	}
	if attrErr != nil {
		err = fmt.Errorf("%v: remove subattr err: %v", attrErr)
	}
	if subErr != nil {
		err = fmt.Errorf("%v: remove conn subid err: %v", subErr)
	}

	return err
}
