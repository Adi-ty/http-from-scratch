package main

import (
	"fmt"
	"log"
	"net"

	"github.com/Adi-ty/http-from-scratch/internal/request"
)

func main() {
	listener, err := net.Listen("tcp", ":42069")
	if err != nil {
		log.Fatal("error", "error", err)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal("error", "error", err)
		}

		r, err := request.RequestFromReader(conn)
		if err != nil {
			log.Fatal("error", "error", err)
		}

		fmt.Printf("Request line:\n")
		fmt.Printf("- Method: %s", r.RequestLine.Method)
		fmt.Printf("\n- Path: %s", r.RequestLine.RequestTarget)
		fmt.Printf("\n- Version: %s\n", r.RequestLine.HttpVersion)
		
		fmt.Printf("Headers:\n")
		r.Headers.Iterate(func(name, value string) {
			fmt.Printf("- %s: %s\n", name, value)
		})
	}
}