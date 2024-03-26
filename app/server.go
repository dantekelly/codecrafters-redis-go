package main

import (
	"fmt"
	"log"
	"strings"

	// Uncomment this block to pass the first stage
	"net"
	"os"
)

type Replica struct {
	Host string
	Port string
}

func main() {
	argArray := os.Args[1:]
	port := "6379"
	replica := Replica{}

	redis := NewRedisServer()
	redis.Config.Role = "master"
	redis.Config.Replica = replica

	for i := 0; i < len(argArray); i++ {
		switch argArray[i] {
		case "--port":
			port = argArray[i+1]
			redis.Config.Port = port
		case "--replicaof":
			replica = Replica{
				Host: argArray[i+1],
				Port: argArray[i+2],
			}
			redis.Config.Replica = replica
			redis.Config.Role = "slave"
		}
	}

	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage
	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", port))
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	defer l.Close()

	fmt.Printf("Server is listening on port %s\n", port)

	if redis.Config.Role == "slave" {
		go connectToMaster(redis)
	}

	for {
		c, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err.Error())
			continue
		}

		log.Println("Accepted connection, handling client")
		go handleClient(c, redis)
	}
}

func connectToMaster(redis *RedisServer) {
	fmt.Println("Connecting to master")

	c, err := net.Dial("tcp", fmt.Sprintf("%s:%s", redis.Config.Replica.Host, redis.Config.Replica.Port))
	if err != nil {
		fmt.Println("Failed to connect to master", err)
		os.Exit(1)
	}
	defer c.Close()

	// 1st Ping
	pingArray := []Value{{
		Type: BulkString,
		Bulk: "ping",
	}}
	encodedArray := encodeArray(pingArray)
	c.Write(encodedArray)
	log.Println("Sent PING to master")

	previousCommands := make([]Value, 0)
	for {
		// lastCommand := previousCommands[len(previousCommands)-1]

		resp := NewResp(c)
		value, err := resp.Parse()
		if err != nil {
			fmt.Println(err)
			return
		}
		previousCommands = append(previousCommands, value)

		if len(previousCommands) == 1 {
			if value.String != "PONG" {
				fmt.Println("Failed to connect to master")
				os.Exit(1)
			}
			log.Println("Received PONG from master")

			replconf1 := []Value{{
				Type: BulkString,
				Bulk: "REPLCONF",
			}, {
				Type: BulkString,
				Bulk: "listening-port",
			}, {
				Type: BulkString,
				Bulk: redis.Config.Port,
			}}
			c.Write(encodeArray(replconf1))
			log.Println("Sent REPLCONF listening-port to master")
		}

		if len(previousCommands) == 2 {
			if value.String != "OK" {
				fmt.Println("Failed to connect to master")
				os.Exit(1)
			}
			log.Println("Received OK from master")

			replconf2 := []Value{{
				Type: BulkString,
				Bulk: "REPLCONF",
			}, {
				Type: BulkString,
				Bulk: "capa",
			}, {
				Type: BulkString,
				Bulk: "psync2",
			}}

			c.Write(encodeArray(replconf2))
			log.Println("Sent REPLCONF capa to master")
		}

		if len(previousCommands) == 3 {
			if value.String != "OK" {
				fmt.Println("Failed to connect to master")
				os.Exit(1)
			}
			log.Println("Received OK from master")

			psyncResp := []Value{{
				Type: BulkString,
				Bulk: "PSYNC",
			}, {
				Type: BulkString,
				Bulk: "?",
			}, {
				Type: BulkString,
				Bulk: "-1",
			}}
			c.Write(encodeArray(psyncResp))

			log.Println("Sent PSYNC to master")
		}
	}

}

func handleClient(c net.Conn, redis *RedisServer) {
	defer c.Close()

	for {
		resp := NewResp(c)
		value, err := resp.Parse()
		if err != nil {
			fmt.Println(err)
			return
		}

		switch value.Type {
		case Array:
			command := strings.ToUpper(value.Array[0].String)

			res, err := RunCommand(redis, command, value.Array[1:])
			if err != nil {
				errorBytes := encodeError(err.Error())
				c.Write(errorBytes)
				return
			}

			c.Write(res)
		default:
			c.Write(encodeString("OK"))
		}
	}
}
