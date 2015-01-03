package main

import ()

func authSubCli(handler Handler) {
	if handler.Request.Method != "POST" {
		ret := map[string]string{
			"status": "error",
			"msg":    "wrong http method",
		}
		RenderJson(handler, ret)
		return
	}
	cliId := handler.Request.FormValue("client_id")
	timestamp := handler.Request.FormValue("timestamp")
	token := handler.Request.FormValue("token")
	if !ValidSubCliToken(cliId, timestamp, Config.SignKey, token) {
		ret := map[string]string{
			"status": "error",
			"msg":    "auth failed",
		}
		RenderJson(handler, ret)
		return
	}
	// TODO: register subscribe client to etcd service
	ret := map[string]string{
		"status": "ok",
		"msg":    "auth success",
	}
	RenderJson(handler, ret)
}
