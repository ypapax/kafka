package main

import (
	"context"
	"github.com/segmentio/kafka-go"
	"log"
	"time"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	consumer()
}

func consumer() {
	// to consume messages
	topic := "my-topic"
	partition := 0
	log.Println("dialing...")
	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}
	log.Println("dialed")
	//if errS := conn.SetReadDeadline(time.Now().Add(10*time.Second)); errS != nil {
	//	log.Fatal(errS)
	//}
	log.Println("set deadline")
	batch := conn.ReadBatch(10e3, 1e6) // fetch 10KB min, 1MB max
	log.Println("listening...")
	b := make([]byte, 10e3) // 10KB max per message
	var i int
	for {
		time.Sleep(1*time.Millisecond)
		n, errR := batch.Read(b)
		if errR != nil {
			log.Printf("err: %+v", errR)
			time.Sleep(time.Second)
			continue
		}
		i++
		log.Printf("got message: %+v", i)
		log.Println(string(b[:n]))

	}

	if err := batch.Close(); err != nil {
		log.Fatal("failed to close batch:", err)
	}

	if err := conn.Close(); err != nil {
		log.Fatal("failed to close connection:", err)
	}
}
