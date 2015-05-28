package main

import (
	"fmt"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"github.com/coreos/go-etcd/etcd"
	"strconv"
	"strings"
)

func getEtcdClient(machines []string) (*etcd.Client, error) {
	c := etcd.NewClient(machines)
	return c, nil
}

func loadIndexBase(c *etcd.Client, idxBase *IndexBase) error {
	if idxBase.attrbases == nil {
		idxBase.attrbases = make(map[string]*AttrBase)
	}

	// load /idx/info/dimension
	dimKey := dmq.GetIndexBaseDim()
	resp, err := c.Get(dimKey, false, false)
	if err != nil {
		return err
	}
	dimension, err := strconv.Atoi(resp.Node.Value)
	if err != nil {
		return err
	}
	idxBase.dimension = dimension

	idxBaseBound := dmq.GetIndexBaseBound()
	if resp, err := c.Get(idxBaseBound, false, true); err != nil {
		return err
	} else if !resp.Node.Dir {
		return fmt.Errorf("%v should be a directory", resp.Node.Key)
	} else {
		// iteration for /idx/info/bound
		for _, attrNameNode := range resp.Node.Nodes {
			if !attrNameNode.Dir {
				return fmt.Errorf("%v should be a directory", attrNameNode.Key)
			}

			lstr, ustr := "", ""
			keySp := strings.Split(attrNameNode.Key, "/")
			attrName := keySp[len(keySp)-1]
			lowerKey := dmq.GetIndexBaseBoundKey(attrName, dmq.IdxAttrLower)
			upperKey := dmq.GetIndexBaseBoundKey(attrName, dmq.IdxAttrUpper)
			// iteration /idx/info/<attr-name> for lower and upper bound
			for _, attrNode := range attrNameNode.Nodes {
				if attrNode.Key == lowerKey {
					if attrNode.Dir {
						return fmt.Errorf("%v should be a directory", attrNode.Key)
					}
					lstr = attrNode.Value
				}
				if attrNode.Key == upperKey {
					if attrNode.Dir {
						return fmt.Errorf("%v should be a directory", attrNode.Key)
					}
					ustr = attrNode.Value
				}
			}

			lower, err := strconv.Atoi(lstr)
			if err != nil {
				return fmt.Errorf("invalid lower bound '%s' for %s", lstr, attrNameNode.Key)
			}
			upper, err := strconv.Atoi(ustr)
			if err != nil {
				return fmt.Errorf("invalid upper bound '%s' for %s", ustr, attrNameNode.Key)
			}

			// update indxbase
			attrbase := &AttrBase{
				name: attrName,
				use:  dmq.AttrUseField["range"],
				low:  lower,
				high: upper,
			}
			idxBase.attrbases[attrName] = attrbase
		}
	}

	return nil
}

func getPnodeBindAddr(c *etcd.Client, pnid string) (string, error) {
	pnkey := dmq.GetDataPNodeKey(pnid)

	statusKey := fmt.Sprintf("%s/%s", pnkey, dmq.DataPnodeStatus)
	if resp, err := c.Get(statusKey, false, false); err != nil {
		return "", err
	} else if resp.Node.Dir {
		return "", fmt.Errorf("%s should not be directory", statusKey)
	} else if resp.Node.Value != dmq.DataNodeStatusActive {
		return "", fmt.Errorf("invalid status %s of pnode", resp.Node.Value)
	}

	paddrKey := fmt.Sprintf("%s/%s", pnkey, dmq.DataPnodePubAddr)
	if resp, err := c.Get(paddrKey, false, false); err != nil {
		return "", err
	} else if resp.Node.Dir {
		return "", fmt.Errorf("%s should not be directory", paddrKey)
	} else {
		return resp.Node.Value, nil
	}
}
