package main

import (
	"fmt"
	"io"
	"net"
	"os"
)

func main() {
	addr := "localhost:9000"
	if len(os.Args) > 1 {
		addr = os.Args[1]
	}

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Println("Не вдалося підключитись:", err)
		return
	}
	defer conn.Close()

	done := make(chan bool)
	go func() {
		io.Copy(os.Stdout, conn)
		done <- true
	}()
	go func() {
		io.Copy(conn, os.Stdin)
		conn.(*net.TCPConn).CloseWrite()
	}()
	<-done
}