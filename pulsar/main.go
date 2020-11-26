package main

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/apache/pulsar-client-go/pulsar"
)

var topic = "my-topic8"

func produce() {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://40.119.250.46:6650",
	})

	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: topic,
		//BatchingMaxMessages: 10,
	})
	for i := 0; i < 10; i++ {
		producer.SendAsync(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte("hello")},
			func(m pulsar.MessageID, p *pulsar.ProducerMessage, e error) {
				fmt.Println("produce error", e)
			},
		)
	}
	producer.Flush()
	defer producer.Close()

	if err != nil {
		fmt.Println("Failed to publish message", err)
	}
	fmt.Println("Published message")
}

func consume(idx int) {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: "pulsar://40.119.250.46:6650",
	})
	if err != nil {
		panic(err)
	}

	defer client.Close()

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       topic,
		SubscriptionName:            "my-sub",
		Type:                        pulsar.Shared,
		ReceiverQueueSize:           1,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
	})
	if err != nil {
		panic(err)
	}

	defer consumer.Close()
	for {
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
			break
		}

		fmt.Printf("Consumer %d, Received message msgId: %#v -- content: '%s'\n",
			idx, msg.ID(), string(msg.Payload()))
	}

}

func main() {
	//produce()
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			consume(idx)
		}(i)
	}
	wg.Wait()
}
