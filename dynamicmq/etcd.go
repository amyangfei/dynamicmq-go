package dynamicmq

import (
	"fmt"
	sherlock "github.com/amyangfei/sherlock-go"
	"github.com/coreos/go-etcd/etcd"
)

func GetInfoBase(connType string) string {
	return fmt.Sprintf("/%s/info", connType)
}

func GetInfoKey(connType, nodeId string) string {
	return fmt.Sprintf("/%s/info/%s", connType, nodeId)
}

func GetWaitingBase(nodeType string) string {
	return fmt.Sprintf("/%s/waiting", nodeType)
}

func GetConnSubKey(connId, subId string) string {
	return fmt.Sprintf("/%s/info/%s/sub/%s", EtcdConnectorType, connId, subId)
}

func GetSubConnKey(clientId string) string {
	return fmt.Sprintf("/%s/info/%s/conn_id", EtcdSubscriberType, clientId)
}

func GetSubAttrBase() string {
	return fmt.Sprintf("/%s/attr", EtcdSubscriberType)
}

func GetSubAttrCliBase(clientId string) string {
	return fmt.Sprintf("%s/%s", GetSubAttrBase(), clientId)
}

func GetSubAttrKey(clientId, attrName string) string {
	return fmt.Sprintf("%s/%s", GetSubAttrCliBase(clientId), attrName)
}

func GetIndexBaseDim() string {
	return fmt.Sprintf("/%s/info/dimension", EtcdIndexInfoType)
}

func GetIndexBaseBound() string {
	return fmt.Sprintf("/%s/info/bound", EtcdIndexInfoType)
}

func GetIndexBaseBoundKey(attrname, lowerOrUpper string) string {
	return fmt.Sprintf("%s/%s/%s", GetIndexBaseBound(), attrname, lowerOrUpper)
}

func GetDataPNodeBase() string {
	return fmt.Sprintf("/%s/%s", EtcdDataNodeType, DataPnode)
}

func GetDataPNodeKey(nodeId string) string {
	return fmt.Sprintf("%s/%s", GetDataPNodeBase(), nodeId)
}

func GetDataVnodeKey() string {
	return fmt.Sprintf("/%s/%s", EtcdDataNodeType, DataVnode)
}

func GetWaitingLockMgr(c *etcd.Client, owner string) *sherlock.EtcdLock {
	l := sherlock.NewEtcdLock("WaitingConnector", c)
	l.SetNamespace("lock")
	l.SetOwner(owner)
	return l
}

func RegisterConnToWaiting(c *etcd.Client, nodeId string) error {
	waitKey := fmt.Sprintf("%s/%s", GetWaitingBase(EtcdConnectorType), nodeId)
	// check whether directory exists
	if resp, err := c.Get(waitKey, false, false); err == nil {
		if resp.Node.Dir {
			return nil
		}
	}
	if _, err := c.SetDir(waitKey, 0); err != nil {
		c.DeleteDir(waitKey)
		return err
	}
	return nil
}

func UnregisterConnToWaiting(c *etcd.Client, nodeId string) error {
	waitKey := fmt.Sprintf("%s/%s", GetWaitingBase(EtcdConnectorType), nodeId)
	_, err := c.DeleteDir(waitKey)
	return err
}

// remove all subscription attribute index base infromation in etcd
func RemoveAttrIndexBase(c *etcd.Client) error {
	idxInfoKey := GetInfoBase(EtcdIndexInfoType)
	_, err := c.Delete(idxInfoKey, true)
	return err
}

func NewAttrIndexBase(c *etcd.Client, dim int, names []string, lower, upper []int) error {
	if len(names) != dim {
		return fmt.Errorf("invalid size of names")
	}
	if len(lower) != dim || len(upper) != dim {
		return fmt.Errorf("invalid size of lower bound or upper bound")
	}
	for i := 0; i < dim; i++ {
		if lower[i] >= upper[i] {
			return fmt.Errorf("invalid lower and upper group, %d >= %d", lower[i], upper[i])
		}
	}

	// first clean existing attribute index space
	idxInfoKey := GetInfoBase(EtcdIndexInfoType)
	if _, err := c.Get(idxInfoKey, false, false); err == nil {
		if err := RemoveAttrIndexBase(c); err != nil {
			return err
		}
	}

	dimKey := GetIndexBaseDim()
	if _, err := c.Set(dimKey, fmt.Sprintf("%d", dim), 0); err != nil {
		return err
	}
	for i := 0; i < dim; i++ {
		lowKey := GetIndexBaseBoundKey(names[i], IdxAttrLower)
		upKey := GetIndexBaseBoundKey(names[i], IdxAttrUpper)
		if _, err := c.Set(lowKey, fmt.Sprintf("%d", lower[i]), 0); err != nil {
			return err
		}
		if _, err := c.Set(upKey, fmt.Sprintf("%d", upper[i]), 0); err != nil {
			return err
		}
	}
	return nil
}
