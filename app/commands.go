package main

import (
	"encoding/hex"
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

// Worker to process commands sequentially
func CommandWorker(redis *RedisServer) {
	defer redis.wg.Done()
	for cmd := range redis.Commands {
		switch cmd.Command.Type {
		case Array:
			commandString := cmd.Command.Array[0].String
			if cmd.Command.Array[0].Type == BulkString {
				commandString = cmd.Command.Array[0].Bulk
			}
			command := strings.ToUpper(commandString)

			res, err := RunCommand(redis, command, cmd.Command.Array[1:])
			if err != nil {
				errorBytes := encodeError(err.Error())

				cmd.Conn.Write(errorBytes)
				return
			}

			_, writeErr := cmd.Conn.Write(res)
			if writeErr != nil {
				log.Printf("Error writing to connection: %v", writeErr)
			}

			if command == "PSYNC" {
				emptyRDB, err := hex.DecodeString("524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2")
				if err != nil {
					log.Println(err)
					return
				}

				cmd.Conn.Write(encodeRDB(string(emptyRDB)))

				redis.Config.Slaves = append(redis.Config.Slaves, &Slave{
					Conn: cmd.Conn,
				})

				log.Println("Connected to slave")
			}
		case String:
		default:
			cmd.Conn.Write(encodeString("OK"))
		}
	}
}

func propogateCommand(redis *RedisServer, command string, args []Value) {
	for _, slave := range redis.Config.Slaves {
		if slave.Conn == nil {
			log.Println("Slave connection is nil, skipping propagation")
			continue
		}

		arr := []Value{
			{
				Type: BulkString,
				Bulk: command,
			},
		}
		arr = append(arr, args...)
		encodedCommand := encodeArray(arr)

		log.Println("Propogating command to slave", slave.Conn.RemoteAddr())

		// Retry mechanism
		retries := 3
		for retries > 0 {
			_, err := slave.Conn.Write(encodedCommand)
			if err != nil {
				log.Printf("Error writing to slave %s: %v", slave.Conn.RemoteAddr(), err)
				retries--
				if retries == 0 {
					log.Printf("Failed to propagate command '%s' to slave %s after retries", command, slave.Conn.RemoteAddr())
				}
			} else {
				break
			}
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

		redis.Set(args[0].Raw, args[1].Raw, expiry)
		propogateCommand(redis, command, args)

		return encodeString("OK"), nil
	case "GET":
		if len(args) != 1 {
			return nil, ErrWrongNumberOfArgsGet
		}

		value, ok := redis.Get(args[0].Raw)
		if !ok {
			return []byte(ErrKeyNotFound), nil
		}

		if value.Expiry > 0 && value.Expiry < time.Now().UnixMilli() {
			redis.Del(args[0].Raw)
			return []byte(ErrKeyNotFound), nil
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

		if len(args) < 1 {
			log.Print("Wrong number of arguments")
			return nil, ErrWrongNumberOfArgsRepl
		}

		firstArg := strings.ToLower(args[0].Raw)

		switch firstArg {
		case "getack":
			response := []Value{
				{
					Type: BulkString,
					Bulk: "REPLCONF",
				},
				{
					Type: BulkString,
					Bulk: "ACK",
				},
				{
					Type: BulkString,
					Bulk: "0",
				},
			}

			return encodeArray(response), nil
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
