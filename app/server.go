package main

import (
	"fmt"
	"log"

	// Uncomment this block to pass the first stage
	"net"
	"os"
)

var requestPing = []byte("*1\r\n$4\r\nping\r\n")
var responsePing = []byte("+PONG\r\n")

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage
	//
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}

	defer l.Close()

	fmt.Println("Server is listening on port 6379")

	for {
		c, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err.Error())
			continue
		}

		log.Println("Accepted connection, handling client")
		go handleClient(c)
	}
}

func handleClient(c net.Conn) {
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
			log.Print("Array received:", value.Array)
			if value.Array[0].String == "ping" {
				c.Write(responsePing)
			} else if value.Array[0].String == "quit" {
				return
			} else if value.Array[0].String == "echo" {
				if len(value.Array) == 2 {
					res := encodeBulkString(value.Array[1].String)
					c.Write([]byte(res))
				} else {
					c.Write([]byte("-ERR wrong number of arguments for 'echo' command\r\n"))
				}
			} else {
				c.Write([]byte("+OK\r\n"))
			}
		default:
			c.Write([]byte("+OK\r\n"))
		}
	}
}
