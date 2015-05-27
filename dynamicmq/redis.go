package dynamicmq

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"strings"
	"time"
)

var (
	redisProtoSep = "@"
	// ErrRedisNoConn represents for no available redis conn error
	ErrRedisNoConn = fmt.Errorf("failed to get a redis conn")
)

// RedisConfig struct
type RedisConfig struct {
	Endpoint  string
	MaxIdle   int
	MaxActive int
	Timeout   int
}

// RedisCliPool struct
type RedisCliPool struct {
	pool *redis.Pool
}

// NewRedisConfig returns a new RedisConfig
func NewRedisConfig(endpoint string, idle, active, timeout int) *RedisConfig {
	return &RedisConfig{
		Endpoint:  endpoint,
		MaxIdle:   idle,
		MaxActive: active,
		Timeout:   timeout,
	}
}

// GetConn returns an available redis.Conn from the given RedisCliPool
func (rc *RedisCliPool) GetConn() redis.Conn {
	// TODO: rc.pool.Get() returns errorConnection if error occurs,
	// in this situation we should return nil
	return rc.pool.Get()
}

// NewRedisCliPool inits a RedisCliPool based on a given RedisConfig
func NewRedisCliPool(cfg *RedisConfig) (*RedisCliPool, error) {
	// get protocol and address
	pa := strings.Split(cfg.Endpoint, redisProtoSep)
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

// GetSubConnID gets the connecter id which is connected by subclient scID
func GetSubConnID(rc *RedisCliPool, scID string) (string, error) {
	subconnKey := GetSubConnKey(scID)
	return RedisKVGet(rc, subconnKey)
}

// RedisKVGet gets the string value for key in redis
func RedisKVGet(rc *RedisCliPool, key string) (string, error) {
	conn := rc.GetConn()
	if conn == nil {
		return "", ErrRedisNoConn
	}
	defer conn.Close()

	r, err := conn.Do("GET", key)
	return redis.String(r, err)
}
