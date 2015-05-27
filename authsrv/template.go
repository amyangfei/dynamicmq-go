package main

import (
	"encoding/json"
)

func renderJSON(handler Handler, data interface{}) {
	d, err := json.Marshal(data)
	if err != nil {
		// log.Error("unable to parse %v", data)
		panic(err)
	}
	handler.RespWriter.Header().Set("Content-Type", "application/json")
	handler.RespWriter.Write(d)
}
