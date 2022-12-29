package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/protocol"
)

func main() {

	forever := make(chan bool)

	// PRODUCER:

	writer := &kafka.Writer{
		Addr:  kafka.TCP("localhost:9094"),
		Topic: "topic1",
	}

	go func() {
		for i := 0; i <= 10; i++ {

			message := fmt.Sprintf("mensagem-%d", i)

			err := writer.WriteMessages(context.Background(), kafka.Message{
				Value: []byte(message),
				Headers: []protocol.Header{
					{
						Key:   "session",
						Value: []byte("1234"),
					},
				},
				Key: []byte("key1"),
			})

			if err != nil {
				log.Fatal("cannot write a message: ", err)
			}

			time.Sleep(2 * time.Second)
		}
	}()

	// Consumindo
	go consumer("topic1", "consumer1")

	<-forever
}

// CONSUMER

func consumer(topic string, consumerId string) {

	fmt.Println("Consumindo topic : [", topic, "]")

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:  []string{"localhost:9094"},
		GroupID:  consumerId,
		Topic:    topic,
		MinBytes: 0,
		MaxBytes: 10e6, //10MB
	})

	defer reader.Close()

	for {
		message, err := reader.ReadMessage(context.Background())
		if err != nil {
			log.Fatal("cannot receive a message: ", err)
			break
		}

		fmt.Println("message received: ", string(message.Value), "[ Key: ", string(message.Key), "]")
	}
}
