package main

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/hex"
	"regexp"
	"strings"
)

func validSubClientID(cliID string) bool {
	r, _ := regexp.Compile("^[0-9a-fA-F]{24}$")
	return r.MatchString(cliID)
}

func signClientID(cliID, timestamp, key string) string {
	h := hmac.New(sha1.New, []byte(key))
	cnt := strings.ToLower(cliID) + timestamp
	h.Write([]byte(cnt))
	return hex.EncodeToString(h.Sum(nil))
}

func validSubCliToken(cliID, timestamp, key, token string) bool {
	expectToken := signClientID(cliID, timestamp, key)
	return token == expectToken
}
