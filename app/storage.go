package main

import "time"

type Item struct {
	Value  string
	Expiry int64
}

type RedisServer struct {
	Database map[string]Item
}

func NewRedisServer() *RedisServer {
	return &RedisServer{Database: make(map[string]Item)}
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
