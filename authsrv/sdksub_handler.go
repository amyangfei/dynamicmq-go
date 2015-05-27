package main

import (
	"fmt"
	"strings"
	"time"
)

// allocate connector, generate token for subscriber
func prepareAuth(handler Handler) {
	ret := make(map[string]string)
	cliID := strings.ToLower(handler.Request.FormValue("client_id"))
	if !validSubClientID(cliID) {
		log.Warning("recv invalid cliID %s", cliID)
		ret = map[string]string{
			"status": "error",
		}
	} else {
		timestamp := fmt.Sprintf("%d", time.Now().Unix())
		token := signClientID(cliID, timestamp, Config.SignKey)
		// TODO: get connector from global config service like etcd
		if connector, err := allocateConnector(Config.EtcdMachines); err != nil {
			ret = map[string]string{
				"status": "error",
			}
		} else {
			ret = map[string]string{
				"status":    "ok",
				"timestamp": timestamp,
				"token":     token,
				"connector": connector,
			}
		}
	}
	renderJSON(handler, ret)
}
