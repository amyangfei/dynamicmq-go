package main

import (
	"fmt"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"github.com/garyburd/redigo/redis"
)

var (
	redisSetnxNotSet = 0
)

func registerSubCli(rc *dmq.RedisCliPool, scID, connID string) error {
	conn := rc.GetConn()
	if conn == nil {
		return dmq.RedisNoConnErr
	}
	defer conn.Close()

	subconnKey := dmq.GetSubConnKey(scID)
	// FIXME: thread-safe issuse
	// ref: http://godoc.org/github.com/garyburd/redigo/redis#hdr-Concurrency
	if _, err := conn.Do("SET", subconnKey, connID); err != nil {
		return err
	}

	return nil
}

func unRegisterSubCli(scID string, rc *dmq.RedisCliPool) error {
	conn := rc.GetConn()
	if conn == nil {
		return dmq.RedisNoConnErr
	}
	defer conn.Close()

	subconnKey := dmq.GetSubConnKey(scID)
	// FIXME: thread-safe issuse
	// ref: http://godoc.org/github.com/garyburd/redigo/redis#hdr-Concurrency
	if _, err := conn.Do("DEL", subconnKey); err != nil {
		return err
	}

	return nil
}

func createSubAttr(cli *SubClient, attr *Attribute, rc *dmq.RedisCliPool) error {
	key := dmq.GetSubAttrKey(cli.id.Hex(), attr.name)
	jsonStr, err := AttrMarshal(attr)
	if err != nil {
		return err
	}

	conn := rc.GetConn()
	if conn == nil {
		return dmq.RedisNoConnErr
	}
	defer conn.Close()

	r, err := conn.Do("SETNX", key, string(jsonStr))
	if err != nil {
		return err
	}
	if r == redisSetnxNotSet {
		return fmt.Errorf("attribute %s already exists", attr.name)
	}

	return nil
}

// UpdateSubAttr succeeds only if the given key already exists.
func UpdateSubAttr(cli *SubClient, attr *Attribute, rc *dmq.RedisCliPool) error {
	key := dmq.GetSubAttrKey(cli.id.Hex(), attr.name)
	jsonStr, err := AttrMarshal(attr)
	if err != nil {
		return err
	}

	conn := rc.GetConn()
	if conn == nil {
		return dmq.RedisNoConnErr
	}
	defer conn.Close()

	if _, err := conn.Do("SET", key, string(jsonStr)); err != nil {
		return err
	}

	return nil
}

func getSubAttr(cli *SubClient, attrname string, rc *dmq.RedisCliPool) (string, error) {
	conn := rc.GetConn()
	if conn == nil {
		return "", dmq.RedisNoConnErr
	}
	defer conn.Close()

	key := dmq.GetSubAttrKey(cli.id.Hex(), attrname)
	resp, err := conn.Do("GET", key)
	return redis.String(resp, err)
}

func removeSubAttr(cli *SubClient, attrname string, rc *dmq.RedisCliPool) error {
	conn := rc.GetConn()
	if conn == nil {
		return dmq.RedisNoConnErr
	}
	defer conn.Close()

	key := dmq.GetSubAttrKey(cli.id.Hex(), attrname)
	if _, err := conn.Do("DEL", key); err != nil {
		return err
	}

	return nil
}

func removeSubAttrs(cli *SubClient, rc *dmq.RedisCliPool) error {
	conn := rc.GetConn()
	if conn == nil {
		return dmq.RedisNoConnErr
	}
	defer conn.Close()

	conn.Send("MULTI")
	for attrName := range cli.attrs {
		key := dmq.GetSubAttrKey(cli.id.Hex(), attrName)
		conn.Send("DEL", key)
	}
	if _, err := conn.Do("EXEC"); err != nil {
		return err
	}

	return nil
}
