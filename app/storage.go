package main

import "time"

type Item struct {
	Value  string
	Expiry int64
}

type Replication struct {
	ID     string
	Offset int
}

type Config struct {
	Port             string
	Replication      Replication
	Role             string
	Connected_slaves uint
	Replica          Replica
}

type RedisServer struct {
	Database map[string]Item
	Config   Config
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
		Database: make(map[string]Item),
	}
}

func (r *RedisServer) Set(key, value string, expiry int) {
	r.Database[key] = Item{Value: value, Expiry: 0}

	if expiry > 0 {
		r.Database[key] = Item{Value: value, Expiry: time.Now().UnixMilli() + int64(expiry)}
	}
}

func (r *RedisServer) Get(key string) (Item, bool) {
	value, ok := r.Database[key]
	return value, ok
}

func (r *RedisServer) Del(key string) {
	delete(r.Database, key)
}
