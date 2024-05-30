package main

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
)

type Replica struct {
	Host string
	Port string
}

/* ISSUE: The following code does not replicate the commands to the slave server reliably. */

func processArgs() (string, *Replica) {
	argArray := os.Args[1:]
	port := "6379"
	replica := &Replica{}

	for i := 0; i < len(argArray); i++ {
		switch argArray[i] {
		case "--port":
			port = argArray[i+1]
		case "--replicaof":
			host := strings.Split(argArray[i+1], " ")[0]
			port := strings.Split(argArray[i+1], " ")[1]
			replica = &Replica{
				Host: host,
				Port: port,
			}
		}
	}

	return port, replica
}

func main() {
	port, replica := processArgs()

	redis := NewRedisServer()
	redis.Config.Role = "master"
	redis.Config.Replica = *replica
	redis.Config.Port = port

	if redis.Config.Replica.Host != "" {
		redis.Config.Role = "slave"
	}

	// You can use print statements as follows for debugging, they'll be visible when running tests.
	log.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage
	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", port))
	if err != nil {
		log.Println("Failed to bind to port", port)
		os.Exit(1)
	}
	defer l.Close()

	// Start command processing worker
	redis.wg.Add(1)
	go CommandWorker(redis)

	if redis.Config.Role == "slave" {
		go handleMaster(redis)
	} else {
		log.Println("[Master] Waiting for connections")
	}

	for {
		c, err := l.Accept()
		if err != nil {
			log.Println("Error accepting connection:", err.Error())
			continue
		}

		log.Println("Accepted connection, handling client")
		go handleClient(c, redis)
	}
}

func handleMaster(redis *RedisServer) {
	log.Println("[Slave] Connecting to master")

	c, err := net.Dial("tcp", fmt.Sprintf("%s:%s", redis.Config.Replica.Host, redis.Config.Replica.Port))
	if err != nil {
		log.Println("[Slave] Failed to connect to master", err)
		os.Exit(1)
	}
	defer func() {
		c.Close()
		log.Println("[Slave] Connection to master closed")
	}()

	// 1st Ping
	connected := false
	pingArray := []Value{{
		Type: BulkString,
		Bulk: "ping",
	}}
	encodedArray := encodeArray(pingArray)
	c.Write(encodedArray)
	step := 1

	resp := NewResp(c)
	for {
		value, err := resp.Parse()
		if err != nil {
			log.Println("[Slave] Error parsing command:", err)
			return
		}

		log.Printf("[Slave] Received command from master: %+v\n", value)

		if !connected {
			if step == 1 && value.Type == String && value.String == "PONG" {
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
				step++

				log.Printf("[Slave] Sending REPLCONF listening-port %s\n", redis.Config.Port)
				c.Write(encodeArray(replconf1))
			} else if step == 2 && value.Type == String && value.String == "OK" {
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

				step++
				log.Println("[Slave] Sending REPLCONF capa psync2")
				c.Write(encodeArray(replconf2))
			} else if step == 3 && value.Type == String && value.String == "OK" {
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

				step++
				c.Write(encodeArray(psyncResp))
			} else if step == 4 && value.Type == String && strings.HasPrefix(value.String, "FULLRESYNC") {
				step++

				log.Println("[Slave] Awaiting RDB File")
			} else if step == 5 && value.Type == BulkString {
				connected = true

				log.Println("[Slave] Received RDB File")
				log.Println("[Slave] Successfully connected to master")
			} else {
				log.Println("[Slave] Failed to connect to master")
				os.Exit(1)
			}
		} else {
			log.Println("[Slave] Sending command to queue")
			redis.Commands <- Command{Conn: c, Command: value}
		}
	}
}

func handleClient(c net.Conn, redis *RedisServer) {
	defer c.Close()

	resp := NewResp(c)
	for {
		value, err := resp.Parse()
		if err != nil {
			if err == io.EOF {
				log.Printf("Client connection closed: %s", c.RemoteAddr())
				return
			}
			log.Println("Error parsing command:", err)
			return
		}

		log.Printf("Sending command to queue: %+v\n", value)
		redis.Commands <- Command{Conn: c, Command: value}
	}
}
