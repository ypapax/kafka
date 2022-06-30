package main

import (
	"context"
	"github.com/segmentio/kafka-go"
	"log"
	"time"
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	producer()
}

func producer() {
	// to produce messages
	topic := "my-topic"
	partition := 0

	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	//conn.SetWriteDeadline(time.Now().Add(10*time.Second))
	messages := []string{"one!", "two!", "three!"}
	for {
		for _, m := range messages {
			_, err = conn.WriteMessages(
				kafka.Message{Value: []byte(m)},
			)
			if err != nil {
				log.Fatal("failed to write messages:", err)
			}
			log.Printf("message is sent: %+v", m)
		}
		time.Sleep(time.Second)
	}



	if err := conn.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}
