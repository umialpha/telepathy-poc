package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

var (
	broker = flag.String("b", "localhost:9092", "broker addr")
	topic  = flag.String("t", "new-topic-test", "topic")
	value  = flag.String("v", "hello", "msg value")
	num    = flag.Int("n", 100, "msg number")
)

func getEvents(p *kafka.Producer) {
	for e := range p.Events() {
		switch ev := e.(type) {
		case *kafka.Message:
			m := ev
			if m.TopicPartition.Error != nil {
				fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
			} else {
				fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
					*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
			}

		default:
			fmt.Printf("Ignored event: %s\n", ev)
		}
	}
}

func sendMsg(p *kafka.Producer, topic string, n int) {
	for i := 0; i < n; i++ {
		value := "Hello Go!"
		p.ProduceChannel() <- &kafka.Message{TopicPartition: kafka.TopicPartition{
			Topic: &topic, Partition: kafka.PartitionAny},
			Value:     []byte(value),
			Timestamp: time.Now()}
	}

}

func createTopic() {
	a, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": *broker})
	if err != nil {
		fmt.Printf("Failed to create Admin client: %s\n", err)
		os.Exit(1)
	}

	// Contexts are used to abort or limit the amount of time
	// the Admin call blocks waiting for a result.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create topics on cluster.
	// Set Admin options to wait for the operation to finish (or at most 60s)
	maxDur, err := time.ParseDuration("60s")
	if err != nil {
		panic("ParseDuration(60s)")
	}
	results, err := a.CreateTopics(
		ctx,
		// Multiple topics can be created simultaneously
		// by providing more TopicSpecification structs here.
		[]kafka.TopicSpecification{{
			Topic:             *topic,
			NumPartitions:     1,
			ReplicationFactor: 1}},
		// Admin options
		kafka.SetAdminOperationTimeout(maxDur))
	if err != nil {
		fmt.Printf("Failed to create topic: %v\n", err)
		os.Exit(1)
	}

	// Print results
	for _, result := range results {
		fmt.Printf("%s\n", result)
	}

	a.Close()
}

func DeleteTopic() error {
	a, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": *broker})
	if err != nil {
		fmt.Println(err)
		return nil
	}
	results, err := a.DeleteTopics(context.Background(), []string{*topic}, kafka.SetAdminOperationTimeout(time.Second*60))
	if err != nil {
		fmt.Printf("Failed to delete topics: %v\n", err)
		return err
	}

	// Print results
	for _, result := range results {
		fmt.Printf("%s\n", result)
	}

	a.Close()
	return nil
}

func Consume(queueName string, groupID string, writeChan chan<- []byte, errCh chan<- error, abortCh <-chan int) {

	defer close(writeChan)
	defer close(errCh)
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":     *broker,
		"broker.address.family": "v4",
		"group.id":              groupID,
		"session.timeout.ms":    6000,
		"auto.offset.reset":     "earliest"})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		errCh <- err
		return
	}
	defer consumer.Close()
	err = consumer.SubscribeTopics([]string{queueName}, nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to SubscribeTopics : %s\n", queueName)
		errCh <- err
		return
	}
	for {
		select {
		case <-abortCh:
			fmt.Printf("Caught abort\n")
			return

		default:
			ev := consumer.Poll(100)
			if ev == nil {
				continue
			}
			switch e := ev.(type) {
			case *kafka.Message:
				fmt.Println("Message Latancy", time.Since(e.Timestamp))
				writeChan <- e.Value
			}
		}
	}

	return
}

func main() {

	flag.Parse()
	startTime := time.Now()
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": *broker, "request.required.acks": 0})

	if err != nil {
		fmt.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Producer %v\n", p)
	go func() {
		createTopic()
		sendMsg(p, *topic, *num)

	}()

	cnt := 0
	writeChan := make(chan []byte, 1000)
	errorChan := make(chan error, 1000)
	abortCh := make(chan int)
	go Consume(*topic, *topic, writeChan, errorChan, abortCh)
	run := true
	for run {
		select {
		case <-writeChan:
			cnt++
			if cnt >= *num {
				run = false
			}
			break
		case <-errorChan:
			break
		}
	}
	abortCh <- 1
	DeleteTopic()
	fmt.Println("Time Cost", time.Since(startTime))
}
