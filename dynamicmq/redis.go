package dynamicmq

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"strings"
	"time"
)

var (
	RedisProtoSep  = "@"
	RedisNoConnErr = fmt.Errorf("failed to get a redis conn")
)

type RedisConfig struct {
	Endpoint  string
	MaxIdle   int
	MaxActive int
	Timeout   int
}

type RedisCliPool struct {
	pool *redis.Pool
}

func NewRedisConfig(endpoint string, idle, active, timeout int) *RedisConfig {
	return &RedisConfig{
		Endpoint:  endpoint,
		MaxIdle:   idle,
		MaxActive: active,
		Timeout:   timeout,
	}
}

func (rc *RedisCliPool) GetConn() redis.Conn {
	// TODO: rc.pool.Get() returns errorConnection if error occurs,
	// in this situation we should return nil
	return rc.pool.Get()
}

func NewRedisCliPool(cfg *RedisConfig) (*RedisCliPool, error) {
	// get protocol and address
	pa := strings.Split(cfg.Endpoint, RedisProtoSep)
	if len(pa) != 2 {
		return nil, fmt.Errorf("error redis endpoint: %s", cfg.Endpoint)
	}
	pool := &redis.Pool{
		MaxIdle:     cfg.MaxIdle,
		MaxActive:   cfg.MaxActive,
		IdleTimeout: time.Duration(cfg.Timeout) * time.Second,
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
	rcpool := &RedisCliPool{pool: pool}
	return rcpool, nil
}

func GetSubConnId(rc *RedisCliPool, scId string) (string, error) {
	conn := rc.GetConn()
	if conn == nil {
		return "", RedisNoConnErr
	}
	defer conn.Close()

	subconnKey := GetSubConnKey(scId)
	r, err := conn.Do("GET", subconnKey)
	return redis.String(r, err)
}
