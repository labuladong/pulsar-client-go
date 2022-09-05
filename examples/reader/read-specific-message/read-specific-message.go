package main

import (
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"log"
)

func main() {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://localhost:6650",
	})

	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	topic := "topic-1"
	ctx := context.Background()

	// create producer
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic:           topic,
		DisableBatching: true,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	// send 10 messages
	msgIDs := [10]pulsar.MessageID{}
	for i := 0; i < 10; i++ {
		msgID, _ := producer.Send(ctx, &pulsar.ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello-%d", i)),
		})
		msgIDs[i] = msgID
	}

	// create reader on 5th message (not included)
	reader, err := client.CreateReader(pulsar.ReaderOptions{
		Topic:                   topic,
		StartMessageID:          msgIDs[4],
		StartMessageIDInclusive: false,
	})

	if err != nil {
		log.Fatal(err)
	}
	defer reader.Close()

	// receive the remaining 5 messages
	for i := 5; i < 10; i++ {
		msg, err := reader.Next(context.Background())
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("Read %d-th msg: %s\n", i, string(msg.Payload()))
	}
}
