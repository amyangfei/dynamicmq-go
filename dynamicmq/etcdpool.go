package dynamicmq

import (
	"fmt"
	"github.com/coreos/go-etcd/etcd"
	"sync"
	"time"
)

// EtcdClientPool struct
type EtcdClientPool struct {
	size      int // current number of etcd clients
	maxsize   int // max number of etcd clients
	counter   int // used for client id generator
	machines  []string
	lock      *sync.Mutex
	idleClis  []*EtcdClient
	usingClis map[int]*EtcdClient
}

// EtcdClient struct
type EtcdClient struct {
	Id      int
	Cli     *etcd.Client
	Lastuse int64
}

// NewEtcdClientPool inits a EtcdClientPool
func NewEtcdClientPool(machines []string, size, maxsize int) *EtcdClientPool {
	if size <= 0 {
		size = 2
	}
	if maxsize < size {
		maxsize = 64
	}
	pool := &EtcdClientPool{
		size:      size,
		maxsize:   maxsize,
		counter:   0,
		machines:  machines[:],
		lock:      new(sync.Mutex),
		idleClis:  make([]*EtcdClient, 0),
		usingClis: make(map[int]*EtcdClient),
	}
	for i := 0; i < size; i++ {
		pool.counter++
		etcdcli := &EtcdClient{
			Id:      pool.counter,
			Cli:     etcd.NewClient(machines),
			Lastuse: time.Now().Unix(),
		}
		pool.idleClis = append(pool.idleClis, etcdcli)
	}
	return pool
}

// this function is not goroutine safe
func (pool *EtcdClientPool) resize() error {
	if pool.size == pool.maxsize {
		return fmt.Errorf("EtcdClientPool has reached max capacity: %d", pool.maxsize)
	}
	newsize := pool.size * 2
	if newsize > pool.maxsize {
		newsize = pool.maxsize
	}
	for i := 0; i < newsize-pool.size; i++ {
		pool.counter++
		etcdcli := &EtcdClient{
			Id:      pool.counter,
			Cli:     etcd.NewClient(pool.machines),
			Lastuse: time.Now().Unix(),
		}
		pool.idleClis = append(pool.idleClis, etcdcli)
	}
	return nil
}

// GetEtcdClient tries to get an available EtcdClient
func (pool *EtcdClientPool) GetEtcdClient() (*EtcdClient, error) {
	pool.lock.Lock()
	defer pool.lock.Unlock()
	if len(pool.idleClis) == 0 {
		if err := pool.resize(); err != nil {
			return nil, err
		}
	}
	cli := pool.idleClis[0]
	pool.idleClis = pool.idleClis[1:]
	pool.usingClis[cli.Id] = cli
	cli.Lastuse = time.Now().Unix()
	return cli, nil
}

// RecycleEtcdClient recycles a EtcdClient with ID of cid
func (pool *EtcdClientPool) RecycleEtcdClient(cid int) error {
	pool.lock.Lock()
	defer pool.lock.Unlock()

	c, ok := pool.usingClis[cid]
	if !ok {
		return fmt.Errorf("EtcdClient id=%d not found", cid)
	}
	delete(pool.usingClis, cid)
	pool.idleClis = append(pool.idleClis, c)

	return nil
}
