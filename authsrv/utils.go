package main

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/hex"
	"regexp"
	"strings"
)

func ValidSubClientId(cliId string) bool {
	r, _ := regexp.Compile("^[0-9a-fA-F]{24}$")
	return r.MatchString(cliId)
}

func SignClientId(cliId, timestamp, key string) string {
	h := hmac.New(sha1.New, []byte(key))
	cnt := strings.ToLower(cliId) + timestamp
	h.Write([]byte(cnt))
	return hex.EncodeToString(h.Sum(nil))
}

func ValidSubCliToken(cliId, timestamp, key, token string) bool {
	expectToken := SignClientId(cliId, timestamp, key)
	return token == expectToken
}
