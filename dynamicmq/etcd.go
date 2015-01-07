package dynamicmq

import (
	"fmt"
)

func GetInfoBase(connType string) string {
	return fmt.Sprintf("/%s/info", connType)
}

func GetInfoKey(connType, nodeId string) string {
	return fmt.Sprintf("/%s/info/%s", connType, nodeId)
}
