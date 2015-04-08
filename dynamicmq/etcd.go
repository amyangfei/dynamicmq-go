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

func GetSubAttrBase(clientId string) string {
	return fmt.Sprintf("/%s/attr/%s", EtcdSubscriberType, clientId)
}

func GetSubAttrKey(clientId, attrName string) string {
	return fmt.Sprintf("%s/%s", GetSubAttrBase(clientId), attrName)
}

func GetWaitingLockMgr(machines []string, owner string) *sherlock.EtcdLock {
	client := etcd.NewClient(machines)
	l := sherlock.NewEtcdLock("WaitingConnector", client)
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
