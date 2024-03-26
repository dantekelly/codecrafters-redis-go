package main

import (
	"errors"
	"log"
	"strconv"
	"strings"
	"time"
)

var (
	ErrWrongNumberOfArgsSet  = errors.New("wrong number of arguments for 'set' command")
	ErrWrongNumberOfArgsEcho = errors.New("wrong number of arguments for 'echo' command")
	ErrWrongNumberOfArgsGet  = errors.New("wrong number of arguments for 'get' command")
	ErrKeyNotFound           = "$-1\r\n"
	ErrUnknownCommand        = errors.New("unknown command")
	ErrBadNumberFormat       = errors.New("value is not an integer or out of range")
)

func RunCommand(redis *RedisServer, command string, args []Value) ([]byte, error) {
	switch command {
	case "PING":
		return encodeString("PONG"), nil
	case "ECHO":
		if len(args) != 1 {
			return []byte(""), ErrWrongNumberOfArgsEcho
		}
		return encodeBulkString(args[0].String), nil
	case "SET":
		if len(args) < 2 || len(args) == 3 || len(args) > 4 {
			return nil, ErrWrongNumberOfArgsSet
		}
		expiry := 0
		if len(args) == 4 {
			mod := strings.ToUpper(args[2].String)
			log.Print("Arguments", args)
			if mod == "PX" {
				exp, err := strconv.Atoi(args[3].String)
				if err != nil {
					log.Print("Error converting string to integer, bad argument", err)
					return nil, ErrBadNumberFormat
				}
				expiry = exp
			}
		}

		redis.Set(args[0].String, args[1].String, expiry)

		return encodeString("OK"), nil
	case "GET":
		if len(args) != 1 {
			return nil, ErrWrongNumberOfArgsGet
		}

		value, ok := redis.Get(args[0].String)
		if !ok {
			return []byte(ErrKeyNotFound), nil
		}
		if value.Expiry > 0 {
			if value.Expiry < time.Now().UnixMilli() {
				redis.Del(args[0].String)
				return []byte(ErrKeyNotFound), nil
			}
		}

		return encodeBulkString(value.Value), nil
	default:
		return encodeString("OK"), nil
	}
}
