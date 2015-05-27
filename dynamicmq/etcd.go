package dynamicmq

import (
	"fmt"
	sherlock "github.com/amyangfei/sherlock-go"
	"github.com/coreos/go-etcd/etcd"
)

// GetInfoBase returns the info base key for specific node type
func GetInfoBase(connType string) string {
	return fmt.Sprintf("/%s/info", connType)
}

// GetInfoKey returns the info key for specific node
func GetInfoKey(connType, nodeID string) string {
	return fmt.Sprintf("/%s/info/%s", connType, nodeID)
}

// GetWaitingBase returns waiting key base
func GetWaitingBase(nodeType string) string {
	return fmt.Sprintf("/%s/waiting", nodeType)
}

// GetSubConnKey returns a subclient's corresponding connector's ID
// TODO: migrate from etcd to redis
func GetSubConnKey(clientID string) string {
	return fmt.Sprintf("/%s/info/%s/conn_id", EtcdSubscriberType, clientID)
}

// GetSubAttrBase returns the key base for subclient's attribute
func GetSubAttrBase() string {
	return fmt.Sprintf("/%s/attr", EtcdSubscriberType)
}

// GetSubAttrCliBase returns the subattr base for subclient
func GetSubAttrCliBase(clientID string) string {
	return fmt.Sprintf("%s/%s", GetSubAttrBase(), clientID)
}

// GetSubAttrKey returns the key for a specific attribute of a subclient
func GetSubAttrKey(clientID, attrName string) string {
	return fmt.Sprintf("%s/%s", GetSubAttrCliBase(clientID), attrName)
}

// GetIndexBaseDim returns the dimension key in etcd
func GetIndexBaseDim() string {
	return fmt.Sprintf("/%s/info/dimension", EtcdIndexInfoType)
}

// GetIndexBaseBound returns base key storing index bound
func GetIndexBaseBound() string {
	return fmt.Sprintf("/%s/info/bound", EtcdIndexInfoType)
}

// GetIndexBaseBoundKey returns the key storing a specific bound of one index
func GetIndexBaseBoundKey(attrname, lowerOrUpper string) string {
	return fmt.Sprintf("%s/%s/%s", GetIndexBaseBound(), attrname, lowerOrUpper)
}

// GetDataPNodeBase returns the datanode's pnode base key
func GetDataPNodeBase() string {
	return fmt.Sprintf("/%s/%s", EtcdDataNodeType, DataPnode)
}

// GetDataPNodeKey returns a specific datanode's pnode key
func GetDataPNodeKey(nodeID string) string {
	return fmt.Sprintf("%s/%s", GetDataPNodeBase(), nodeID)
}

// GetDataVnodeKey returns the datanode's vnode base key
func GetDataVnodeKey() string {
	return fmt.Sprintf("/%s/%s", EtcdDataNodeType, DataVnode)
}

// GetWaitingLockMgr returns a sherlock.EtcdLock for waiting connector register
func GetWaitingLockMgr(c *etcd.Client, owner string) *sherlock.EtcdLock {
	l := sherlock.NewEtcdLock("WaitingConnector", c)
	l.SetNamespace("lock")
	l.SetOwner(owner)
	return l
}

// RegisterConnToWaiting registers a connector's ID to waiting list in etcd
func RegisterConnToWaiting(c *etcd.Client, nodeID string) error {
	waitKey := fmt.Sprintf("%s/%s", GetWaitingBase(EtcdConnectorType), nodeID)
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

// UnregisterConnToWaiting unregisters a connector's ID from waiting list
func UnregisterConnToWaiting(c *etcd.Client, nodeID string) error {
	waitKey := fmt.Sprintf("%s/%s", GetWaitingBase(EtcdConnectorType), nodeID)
	_, err := c.DeleteDir(waitKey)
	return err
}

// RemoveAttrIndexBase removes all subscription attribute index base
// information in etcd
func RemoveAttrIndexBase(c *etcd.Client) error {
	idxInfoKey := GetInfoBase(EtcdIndexInfoType)
	_, err := c.Delete(idxInfoKey, true)
	return err
}

// NewAttrIndexBase inits the subscription attribute base space
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
