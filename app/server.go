package main

import (
	"fmt"
	"log"
	"strings"

	// Uncomment this block to pass the first stage
	"net"
	"os"
)

func main() {
	argArray := os.Args[1:]
	port := "6379"

	if len(argArray) == 2 {
		if argArray[0] == "--port" {
			port = argArray[1]
		}
	}

	redis := NewRedisServer()
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage
	//
	l, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", port))
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	defer l.Close()

	fmt.Printf("Server is listening on port %s\n", port)

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

			if command == "quit" {
				return
			}

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
