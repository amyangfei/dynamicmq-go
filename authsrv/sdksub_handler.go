package main

import (
	"fmt"
	"strings"
	"time"
)

func requestToken(handler Handler) {
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
		ret = map[string]string{
			"status":    "ok",
			"timestamp": timestamp,
			"token":     token,
		}
	}
	RenderJson(handler, ret)
}
