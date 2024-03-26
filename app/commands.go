package main

import (
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"
)

var (
	ErrWrongNumberOfArgsSet   = errors.New("wrong number of arguments for 'set' command")
	ErrWrongNumberOfArgsEcho  = errors.New("wrong number of arguments for 'echo' command")
	ErrWrongNumberOfArgsGet   = errors.New("wrong number of arguments for 'get' command")
	ErrWrongNumberOfArgsPsync = errors.New("wrong number of arguments for 'psync' command")
	ErrKeyNotFound            = "$-1\r\n"
	ErrUnknownCommand         = errors.New("unknown command")
	ErrBadNumberFormat        = errors.New("value is not an integer or out of range")
)

func RunCommand(redis *RedisServer, command string, args []Value) ([]byte, error) {
	command = strings.ToUpper(command)
	command = strings.Trim(command, "\r\n")

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
	case "INFO":
		if len(args) == 1 {
			if args[0].String == "replication" {
				config := redis.Config
				configString := fmt.Sprintf("# Replication\n")
				configString += fmt.Sprintf("role:%s\n", config.Role)
				if config.Role == "master" {
					// configString += "\nconnected_slaves:" + strconv.Itoa(int(config.Connected_slaves))
					configString += fmt.Sprintf("master_replid:%s\n", config.Replication.ID)
					configString += fmt.Sprintf("master_repl_offset:%s\n", strconv.Itoa(config.Replication.Offset))
				}

				return encodeBulkString(configString), nil
			}
			return encodeBulkString("redis_version:0.0.1"), nil
		}

		return encodeString("redis_version:0.0.1"), nil
	case "REPLCONF":
		if args[0].Type != BulkString {
			log.Print("Invalid argument")
			return nil, errors.New("invalid argument")
		}
		if len(args) < 2 || len(args) > 3 {
			log.Print("Wrong number of arguments")
			return nil, errors.New("wrong number of arguments")
		}

		if args[0].String == "listening-port" {
			return encodeString("OK"), nil
		}
		if args[0].String == "capa" {
			return encodeString("OK"), nil
		}

		return nil, errors.New("unknown argument")
	case "PSYNC":
		if len(args) != 2 {
			return nil, ErrWrongNumberOfArgsPsync
		}

		if args[0].String == "?" && args[1].String == "-1" {
			fullResString := fmt.Sprintf("FULLRESYNC %s 0", redis.Config.Replication.ID)
			return encodeString(fullResString), nil
		}

		return nil, errors.New("arguments are invalid")

	default:
		fmt.Println("Unknown command", command)
		return encodeString("OK"), nil
	}
}
