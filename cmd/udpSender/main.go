package main

import (
	"bufio"
	"fmt"
	"log"
	"net"
	"os"
)

func main() {
	addr, err := net.ResolveUDPAddr("udp", "localhost:42069")
	if err != nil {
		panic(err)
	}

	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Println("> ")

		line, err := reader.ReadString('\n')
		if err != nil {
			log.Println("read error: ", err)
			continue
		}

		_, err = conn.Write([]byte(line))
		if err != nil {
			log.Println("write error: ", err)
			continue
		}
	}
}