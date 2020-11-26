package main

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
)

func publishBeforeSharedConsumer() {
	topic := fmt.Sprintf("publishBeforeSharedConsumer-%v", time.Now())
	sub := "sub"
	client, err := pulsar.NewClient(pulsar.ClientOptions{URL: "pulsar://40.119.250.46:6650"})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: topic,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	msgCnt := 10
	for i := 0; i < msgCnt; i++ {
		producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte("hello")})
	}

	consume := func() {
		consumer, err := client.Subscribe(pulsar.ConsumerOptions{
			Topic:                       topic,
			SubscriptionName:            sub,
			Type:                        pulsar.Shared,
			SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
		})
		if err != nil {
			log.Fatal(err)
		}
		defer consumer.Close()
		for {
			msg, err := consumer.Receive(context.Background())
			if err != nil {
				log.Fatal(err)
			}

			fmt.Printf("Received message msgId: %#v -- content: '%s'\n",
				msg.ID(), string(msg.Payload()))

			consumer.Ack(msg)
		}

	}

	go consume()
	time.Sleep(time.Second * 2)
	consume()

}

func consumeSample() {
	client, err := pulsar.NewClient(pulsar.ClientOptions{URL: "pulsar://40.119.250.46:6650"})
	if err != nil {
		log.Fatal(err)
	}

	defer client.Close()

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:             "test-client-2",
		SubscriptionName:  "sub1",
		Type:              pulsar.Shared,
		ReceiverQueueSize: 1,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()

	for {
		time.Sleep(time.Second * 10)
		msg, err := consumer.Receive(context.Background())
		if err != nil {
			log.Fatal(err)
		}

		fmt.Printf("Received message msgId: %#v -- content: '%s'\n",
			msg.ID(), string(msg.Payload()))

		consumer.Ack(msg)
	}

	if err := consumer.Unsubscribe(); err != nil {
		log.Fatal(err)
	}
}

func main() {
	consumeSample()
}
