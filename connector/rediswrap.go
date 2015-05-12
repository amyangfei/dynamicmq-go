package main

import (
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
)

func RegisterSubCli(rc *dmq.RedisCliPool, scId, connId string) error {
	conn := rc.GetConn()
	if conn == nil {
		return dmq.RedisNoConnErr
	}
	defer conn.Close()

	subconnKey := dmq.GetSubConnKey(scId)
	// FIXME: thread-safe issuse
	// ref: http://godoc.org/github.com/garyburd/redigo/redis#hdr-Concurrency
	if _, err := conn.Do("SET", subconnKey, connId); err != nil {
		return err
	}

	return nil
}

func UnRegisterSubCli(rc *dmq.RedisCliPool, scId string) error {
	conn := rc.GetConn()
	if conn == nil {
		return dmq.RedisNoConnErr
	}
	defer conn.Close()

	subconnKey := dmq.GetSubConnKey(scId)
	// FIXME: thread-safe issuse
	// ref: http://godoc.org/github.com/garyburd/redigo/redis#hdr-Concurrency
	if _, err := conn.Do("DEL", subconnKey); err != nil {
		return err
	}

	return nil
}
