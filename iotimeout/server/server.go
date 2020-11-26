package main

import (
	"fmt"
	"net"
)

func handle(conn net.Conn) {
	defer conn.Close()
	for {
		
	}
}

func main() {
	server, err := net.Listen("tcp", ":5380")
	if err != nil {
		fmt.Println(err)
		return
	}
	for {
		conn, _ := server.Accept()
		go handle(conn)
	}

}
