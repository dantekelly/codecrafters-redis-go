package main

import "errors"

var (
	ErrWrongNumberOfArgsSet  = errors.New("wrong number of arguments for 'set' command")
	ErrWrongNumberOfArgsEcho = errors.New("wrong number of arguments for 'echo' command")
	ErrWrongNumberOfArgsGet  = errors.New("wrong number of arguments for 'get' command")
	ErrKeyNotFound           = "$-1\r\n"
	ErrUnknownCommand        = errors.New("unknown command")
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
		if len(args) != 2 {
			return nil, ErrWrongNumberOfArgsSet
		}
		redis.Database[args[0].String] = args[1].String
		return encodeString("OK"), nil
	case "GET":
		if len(args) != 1 {
			return nil, ErrWrongNumberOfArgsGet
		}
		value, ok := redis.Database[args[0].String]
		if !ok {
			return []byte(ErrKeyNotFound), nil
		}

		return encodeBulkString(value), nil
	default:
		return encodeString("OK"), nil
	}
}
