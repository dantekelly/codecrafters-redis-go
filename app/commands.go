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
	ErrWrongNumberOfArgsRepl  = errors.New("wrong number of arguments for 'replconf' command")
	ErrKeyNotFound            = "$-1\r\n"
	ErrUnknownCommand         = errors.New("unknown command")
	ErrBadNumberFormat        = errors.New("value is not an integer or out of range")
	ErrInvalidArgument        = errors.New("invalid argument")
)

func propogateCommand(redis *RedisServer, command string, args []Value) {
	for _, slave := range redis.Config.Slaves {
		arr := make([]Value, 0)
		arr = append(arr, Value{
			Type: BulkString,
			Bulk: command,
		})
		arr = append(arr, args...)
		encodedCommand := encodeArray(arr)

		log.Println("Propogating command to slave", slave.Conn.RemoteAddr())
		_, err := slave.Conn.Write(encodedCommand)
		if err != nil {
			log.Print("Error writing to slave", err)
		}
	}
}

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

		return encodeBulkString(args[0].Raw), nil
	case "SET":
		if len(args) < 2 || len(args) == 3 || len(args) > 4 {
			return nil, ErrWrongNumberOfArgsSet
		}

		expiry := 0

		if len(args) == 4 {
			mod := strings.ToUpper(args[2].Raw)
			if mod == "PX" {
				exp, err := strconv.Atoi(args[3].Raw)
				if err != nil {
					log.Print("Error converting string to integer, bad argument", err)
					return nil, ErrBadNumberFormat
				}
				expiry = exp
			}
		}

		go redis.Set(args[0].Raw, args[1].Raw, expiry)

		go propogateCommand(redis, command, args)
		return encodeString("OK"), nil
	case "GET":
		if len(args) != 1 {
			return nil, ErrWrongNumberOfArgsGet
		}

		value, ok := redis.Get(args[0].Raw)
		if !ok {
			return []byte(ErrKeyNotFound), nil
		}

		if value.Expiry > 0 {
			if value.Expiry < time.Now().UnixMilli() {
				redis.Del(args[0].Raw)
				return []byte(ErrKeyNotFound), nil
			}
		}

		return encodeBulkString(value.Value), nil
	case "INFO":
		if len(args) == 1 && args[0].Raw == "replication" {
			config := redis.Config

			configString := "# Replication\n"
			configString += fmt.Sprintf("role:%s\n", config.Role)

			if config.Role == "master" {
				configString += fmt.Sprintf("master_replid:%s\n", config.Replication.ID)
				configString += fmt.Sprintf("master_repl_offset:%s\n", strconv.Itoa(config.Replication.Offset))
			}

			return encodeBulkString(configString), nil
		}

		return encodeString("redis_version:0.0.1"), nil
	case "REPLCONF":
		if args[0].Type != BulkString {
			log.Print("Invalid argument")
			return nil, ErrInvalidArgument
		}
		if len(args) < 2 || len(args) > 3 {
			log.Print("Wrong number of arguments")
			return nil, ErrWrongNumberOfArgsRepl
		}

		switch args[0].Raw {
		case "listening-port":
			return encodeString("OK"), nil
		case "capa":
			return encodeString("OK"), nil
		default:
			return nil, ErrInvalidArgument
		}
	case "PSYNC":
		if len(args) != 2 {
			return nil, ErrWrongNumberOfArgsPsync
		}

		if args[0].Raw == "?" && args[1].Raw == "-1" {
			fullResString := fmt.Sprintf("FULLRESYNC %s 0", redis.Config.Replication.ID)

			return encodeString(fullResString), nil
		}

		return nil, ErrInvalidArgument
	default:
		log.Println("Unknown command", command)

		return encodeString("OK"), nil
	}
}
