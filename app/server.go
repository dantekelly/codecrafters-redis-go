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
		handleClient(c)
	}
}

func handleClient(c net.Conn) {
	defer c.Close()

	for {
		buf := make([]byte, 1024)
		n, err := c.Read(buf)
		if err != nil {
			continue
		}

		response := buf[:n]
		if string(response) == string(requestPing) {
			log.Println("Received ping")
			c.Write(responsePing)
		}
	}
}
