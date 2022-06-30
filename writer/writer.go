package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"github.com/tjarratt/babble"
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
	babbler := babble.NewBabbler()
	babbler.Separator = " "
	//conn.SetWriteDeadline(time.Now().Add(10*time.Second))
	var i int
	for {
		i++
		m := fmt.Sprintf("%+v - %+v ", i, babbler.Babble())
		_, err = conn.WriteMessages(
			kafka.Message{Value: []byte(m)},
		)
		if err != nil {
			log.Fatal("failed to write messages:", err)
		}
		log.Printf("message is sent: %+v", m)
		time.Sleep(3 * time.Second)
	}



	if err := conn.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}
