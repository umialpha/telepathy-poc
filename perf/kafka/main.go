package main

import (
	"context"
	crand "crypto/rand"
	"flag"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"time"

	"perf/metric"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func newUUID() (string, error) {
	uuid := make([]byte, 16)
	n, err := io.ReadFull(crand.Reader, uuid)
	if n != len(uuid) || err != nil {
		return "", err
	}
	uuid[8] = uuid[8]&^0xc0 | 0x80
	uuid[6] = uuid[6]&^0xf0 | 0x40
	return fmt.Sprintf("%x-%x-%x-%x-%x", uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:]), nil
}

var (
	addr        = flag.String("b", "bootstrap.kafka.svc.cluster.local:9092", "broker address")
	topic       = flag.String("t", "bench-test", "topic")
	msgSize     = flag.Int("m", 1, "message size in byte")
	mode        = flag.String("mode", "p", "test mode, c / p")
	warmDur     = flag.Int("w", 10, "warm up duration in seconds")
	numMessages = flag.Int("n", 1000000, "message num")
)

func createTopic() {
	a, err := kafka.NewAdminClient(&kafka.ConfigMap{"bootstrap.servers": *addr})
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
			NumPartitions:     3,
			ReplicationFactor: 2}},
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

func produce() {
	createTopic()
	value := make([]byte, *msgSize)
	rand.Read(value)
	var p, err = kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": *addr,
		"linger.ms":             100,
		"request.required.acks": 0})
	if err != nil {
		log.Printf("could not set up kafka producer: %s", err.Error())
		os.Exit(1)
	}
	done := make(chan bool)
	startTime := time.Now().Add(time.Duration(*warmDur) * time.Second)
	hist := metric.NewHistogram()
	go func() {
		for e := range p.Events() {
			switch e.(type) {
			case *kafka.Message:
				msg := e.(*kafka.Message)
				if msg.TopicPartition.Error != nil {
					log.Printf("delivery report error: %v", msg.TopicPartition.Error)
					os.Exit(1)
				}
				if time.Now() >= startTime {
					msgCount++
					if msgCount >= *numMessages {
						done <- true
						return
					}
				}
			}
		}
	}()

	defer p.Close()

	for j := 0; j < *numMessages; j++ {
		p.ProduceChannel() <- &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: topic, Partition: kafka.PartitionAny}, Value: value}
	}
	<-done
	elapsed := time.Since(startTime)

	log.Printf("[confluent-kafka-go producer] msg/s: %f", (float64(*numMessages) / elapsed.Seconds()))

}

func consume() {

	group, _ := newUUID()

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               *addr,
		"group.id":                        group,
		"session.timeout.ms":              6000,
		"go.application.rebalance.enable": false,
		"enable.auto.commit":              false,
		"auto.offset.reset":               "earliest",
	})

	if err != nil {
		log.Printf("could not set up kafka consumer: %s", err.Error())
		os.Exit(1)
	}

	c.SubscribeTopics([]string{*topic}, nil)

	startTime := time.Now().Add(time.Duration(*warmDur) * time.Second)

	var msgCount = 0
	for msgCount < *numMessages {
		ev := c.Poll(100)
		if ev == nil {
			continue
		}
		switch e := ev.(type) {

		case *kafka.Message:
			if time.Now() > startTime {
				msgCount++
				break
			}

		case kafka.PartitionEOF:
			fmt.Printf("%% Reached %v\n", e)
		case kafka.Error:
			fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
			os.Exit(1)
		default:
			fmt.Printf("Ignored %v\n", e)
			os.Exit(1)
		}
	}

	elapsed := time.Since(startTime)

	log.Printf("[conflunet-kafka-go consumer] msg/s: %f", (float64(*numMessages) / elapsed.Seconds()))
}

func main() {
	flag.Parse()
	if *mode == "p" {
		produce()
	} else {
		consume()
	}
}
