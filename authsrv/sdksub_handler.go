package main

import (
	"fmt"
	"strings"
	"time"
)

// allocate connector, generate token for subscriber
func prepareAuth(handler Handler) {
	ret := make(map[string]string)
	cliId := strings.ToLower(handler.Request.FormValue("client_id"))
	if !ValidSubClientId(cliId) {
		log.Warning("recv invalid cliId %s", cliId)
		ret = map[string]string{
			"status": "error",
		}
	} else {
		timestamp := fmt.Sprintf("%d", time.Now().Unix())
		token := SignClientId(cliId, timestamp, Config.SignKey)
		// TODO: get connector from global config service like etcd
		connector := "localhost:7253"
		ret = map[string]string{
			"status":    "ok",
			"timestamp": timestamp,
			"token":     token,
			"connector": connector,
		}
	}
	RenderJson(handler, ret)
}
