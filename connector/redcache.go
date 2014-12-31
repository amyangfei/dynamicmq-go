package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/garyburd/redigo/redis"
	"strings"
	"time"
)

var (
	redisProtoSep     = "@"
	redisMsgCacheSep  = "/"
	redisMsgCacheHead = "rawmsg"
	RedisNoConnErr    = errors.New("failed to get a redis conn")
)

type RedisCache struct {
	connNodeId string
	pool       *redis.Pool
}

func GetMsgKeyInCache(nodeId, msgId string) string {
	return fmt.Sprintf("%s%s%s%s%s",
		redisMsgCacheHead, redisMsgCacheSep, nodeId, redisMsgCacheSep, msgId)
}

func (rc *RedisCache) GetConn() redis.Conn {
	// TODO: rc.pool.Get() returns errorConnection if error occurs,
	// in this situation we should return nil
	return rc.pool.Get()
}

func (rc *RedisCache) SaveRawMsg(rmsg *RawMsg, ttl int) error {
	(*rmsg).Expire = time.Now().Unix() + int64(ttl)
	jsonMsg, err := json.Marshal(rmsg)
	if err != nil {
		return err
	}
	conn := rc.GetConn()
	if conn == nil {
		return RedisNoConnErr
	}
	defer conn.Close()
	msgKey := GetMsgKeyInCache(rc.connNodeId, rmsg.Id)
	if err := conn.Send("SETEX", msgKey, ttl, string(jsonMsg)); err != nil {
		return err
	}
	if err = conn.Flush(); err != nil {
		return err
	}
	if _, err = conn.Receive(); err != nil {
		return err
	}
	return nil
}

func (rc *RedisCache) SaveMultiRawMsg(rmsgs []*RawMsg, ttl int) error {
	conn := rc.GetConn()
	if conn == nil {
		return RedisNoConnErr
	}
	defer conn.Close()

	for _, rmsg := range rmsgs {
		rmsg.Expire = time.Now().Unix() + int64(ttl)
		jsonMsg, err := json.Marshal(rmsg)
		if err != nil {
			return err
		}
		msgKey := GetMsgKeyInCache(rc.connNodeId, rmsg.Id)
		if err := conn.Send("SETEX", msgKey, ttl, string(jsonMsg)); err != nil {
			return err
		}
	}
	if err := conn.Flush(); err != nil {
		return err
	}
	for i := 0; i < len(rmsgs); i++ {
		if _, err := conn.Receive(); err != nil {
			return err
		}
	}
	return nil
}

func (rc *RedisCache) GetRawMsg(id string) (*RawMsg, error) {
	conn := rc.GetConn()
	if conn == nil {
		return nil, RedisNoConnErr
	}
	defer conn.Close()
	now := time.Now().Unix()
	msgKey := GetMsgKeyInCache(rc.connNodeId, id)
	values, err := redis.Bytes(conn.Do("Get", msgKey))
	if err != nil {
		return nil, err
	}
	rmsg := &RawMsg{}
	if err = json.Unmarshal(values, rmsg); err != nil {
		return nil, err
	}
	if rmsg.Expire < now {
		return nil, errors.New("message has expired")
	}
	return rmsg, nil
}

func (rc *RedisCache) DelRawMsg(id string) error {
	conn := rc.GetConn()
	if conn == nil {
		return RedisNoConnErr
	}
	defer conn.Close()
	msgKey := GetMsgKeyInCache(rc.connNodeId, id)
	if _, err := conn.Do("DEL", msgKey); err != nil {
		return err
	}
	return nil
}

func NewRedisCache(cfg MsgCacheConfig) (*RedisCache, error) {
	ConnNodeId, ok := cfg.Data["ConnNodeId"].(string)
	if !ok {
		return nil, errors.New("MsgCacheConfig with error Connector nodeid")
	}

	Endpoint, ok := cfg.Data["Endpoint"].(string)
	if !ok {
		return nil, errors.New("MsgCacheConfig with error redis Endpoint")
	}

	MaxIdle, ok := cfg.Data["MaxIdle"].(int)
	if !ok {
		return nil, errors.New("MsgCacheConfig with error redis MaxIdle")
	}

	MaxActive, ok := cfg.Data["MaxActive"].(int)
	if !ok {
		return nil, errors.New("MsgCacheConfig with error redis MaxActive")
	}

	Timeout, ok := cfg.Data["Timeout"].(int)
	if !ok {
		return nil, errors.New("MsgCacheConfig with error redis Timeout")
	}

	// get protocol and address
	pa := strings.Split(Endpoint, redisProtoSep)
	if len(pa) != 2 {
		return nil, errors.New(
			fmt.Sprintf("error redis endpoint: %s", Endpoint))
	}
	pool := &redis.Pool{
		MaxIdle:     MaxIdle,
		MaxActive:   MaxActive,
		IdleTimeout: time.Duration(Timeout) * time.Second,
		Dial: func() (redis.Conn, error) {
			conn, err := redis.Dial(pa[0], pa[1])
			if err != nil {
				return nil, err
			}
			return conn, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
	c := pool.Get()
	defer c.Close()
	_, err := c.Do("PING")
	if err != nil {
		return nil, err
	}
	rCache := &RedisCache{pool: pool, connNodeId: ConnNodeId}
	return rCache, nil
}
