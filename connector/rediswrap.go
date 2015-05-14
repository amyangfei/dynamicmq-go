package main

import (
	"fmt"
	dmq "github.com/amyangfei/dynamicmq-go/dynamicmq"
	"github.com/garyburd/redigo/redis"
)

var (
	RedisSetnxNotSet = 0
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

func UnRegisterSubCli(scId string, rc *dmq.RedisCliPool) error {
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

func CreateSubAttr(cli *SubClient, attr *Attribute, rc *dmq.RedisCliPool) error {
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
	if r == RedisSetnxNotSet {
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

func GetSubAttr(cli *SubClient, attrname string, rc *dmq.RedisCliPool) (string, error) {
	conn := rc.GetConn()
	if conn == nil {
		return "", dmq.RedisNoConnErr
	}
	defer conn.Close()

	key := dmq.GetSubAttrKey(cli.id.Hex(), attrname)
	if resp, err := conn.Do("GET", key); err != nil {
		return "", err
	} else {
		return redis.String(resp, err)
	}
}

func RemoveSubAttr(cli *SubClient, attrname string, rc *dmq.RedisCliPool) error {
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

func RemoveSubAttrs(cli *SubClient, rc *dmq.RedisCliPool) error {
	conn := rc.GetConn()
	if conn == nil {
		return dmq.RedisNoConnErr
	}
	defer conn.Close()

	conn.Send("MULTI")
	for attrName, _ := range cli.attrs {
		key := dmq.GetSubAttrKey(cli.id.Hex(), attrName)
		conn.Send("DEL", key)
	}
	if _, err := conn.Do("EXEC"); err != nil {
		return err
	}

	return nil
}
