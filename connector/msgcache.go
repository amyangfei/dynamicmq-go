package main

var (
	MsgCache RawMessageCache
)

type RawMsg struct {
	Id      string            `json:"id"`
	Payload string            `json:"payload"`
	Extra   map[string]string `json:"extra",omitempty`
	Expire  int64             `json:"expire"`
}

type RawMessageCache interface {
	SaveRawMsg(rmsg *RawMsg, ttl int) error
	SaveMultiRawMsg(rmsgs []*RawMsg, ttl int) error
	GetRawMsg(id string) (*RawMsg, error)
	DelRawMsg(id string) error
}

type MsgCacheConfig struct {
	Data map[string]interface{}
}

func InitRawMsgCache() error {
	var err error
	cfg := MsgCacheConfig{
		Data: map[string]interface{}{
			"ConnNodeId": Config.NodeId,
			"Endpoint":   Config.RedisEndPoint,
			"MaxIdle":    Config.RedisMaxIdle,
			"MaxActive":  Config.RedisMaxActive,
			"Timeout":    Config.RedisIdleTimeout,
		},
	}
	MsgCache, err = NewRedisCache(cfg)
	return err
}
