package main

import (
	"log"
	"net"
	"time"
)

const CYCLES = 10000000
const ADDRESS = "localhost:8081"

func GenerateDatagrams(sink chan string) {
	DATAGRAMS := []string{
		"stats.a:1|c\n^07.b:2|c\n^09.c:3|c\n^0b.timer:1|ms",
	}
	for {
		for _, dgram := range DATAGRAMS {
			sink <- dgram
		}
	}
}

func SendDatagrams(source chan string, dest string, cycles int) {
	addr, _ := net.ResolveUDPAddr("udp", dest)
	conn, _ := net.DialUDP("udp", nil, addr)
	i := 0
	for dgram := range source {
		conn.Write([]byte(dgram))
		i += 1
		if i >= cycles {
			break
		}
		if i > 0 && i%1000000 == 0 {
			log.Printf("sent %d datagrams\n", i)
		}
		time.Sleep(time.Duration(1) * time.Microsecond)
	}
}

func main() {
	datagrams := make(chan string, 1)
	go GenerateDatagrams(datagrams)
	SendDatagrams(datagrams, ADDRESS, CYCLES)
	log.Printf("Done!\n")
}
