package main

import (
	"net"
	"sync"
	"time"
)

type Item struct {
	Value  string
	Expiry int64
}

type Replication struct {
	ID     string
	Offset int
}

type Slave struct {
	Conn net.Conn
	c    chan string
}

type Config struct {
	Port             string
	Replication      Replication
	Role             string
	Connected_slaves uint
	Slaves           []*Slave
	Replica          Replica
}

type RedisServer struct {
	Store  map[string]Item
	Mutex  sync.RWMutex
	Config Config
}

func NewRedisServer() *RedisServer {
	return &RedisServer{
		Config: Config{
			Port:             "6379",
			Role:             "master",
			Connected_slaves: 0,
			Replication: Replication{
				ID:     "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
				Offset: 0,
			},
		},
		Store: make(map[string]Item),
		Mutex: sync.RWMutex{},
	}
}

func (r *RedisServer) Set(key, value string, expiry int) {
	r.Mutex.Lock()
	defer r.Mutex.Unlock()
	r.Store[key] = Item{Value: value, Expiry: 0}

	if expiry > 0 {
		r.Store[key] = Item{Value: value, Expiry: time.Now().UnixMilli() + int64(expiry)}
	}
}

func (r *RedisServer) Get(key string) (Item, bool) {
	r.Mutex.Lock()
	defer r.Mutex.Unlock()

	value, ok := r.Store[key]

	return value, ok
}

func (r *RedisServer) Del(key string) {
	r.Mutex.Lock()
	defer r.Mutex.Unlock()

	delete(r.Store, key)
}
