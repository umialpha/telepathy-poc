// Example function-based high-level Apache Kafka consumer
package main

/**
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// consumer_example implements a consumer using the non-channel Poll() API
// to retrieve messages and events.

import (
	"fmt"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"os"
	"os/signal"
	"syscall"
)

func consume(addr string, group string, topic string, abort <-chan int) <-chan []byte {
	ch := make(chan []byte, 1000)

	go func() {

		consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers":     addr,
			"broker.address.family": "v4",
			"group.id":              group,
			"session.timeout.ms":    6000,
			"auto.offset.reset":     "earliest"})
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
			ch <- nil
			return
		}
		defer consumer.Close()
		err = consumer.SubscribeTopics([]string{topic}, nil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to SubscribeTopics : %s\n", topic)
			ch <- nil
			return
		}
		for {
			select {
			case <-abort:
				fmt.Printf("Caught abort")
				close(ch)
				return
			default:
				ev := consumer.Poll(100)
				if ev == nil {
					continue
				}
				switch e := ev.(type) {
				case *kafka.Message:
					fmt.Println("Got Message")
					ch <- e.Value
				}
			}
		}
	}()
	return ch
}

func main() {

	if len(os.Args) < 4 {
		fmt.Fprintf(os.Stderr, "Usage: %s <broker> <group> <topics..>\n",
			os.Args[0])
		os.Exit(1)
	}

	broker := os.Args[1]
	group := os.Args[2]
	topics := os.Args[3]
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)
	abort := make(chan int)
	ch := consume(broker, group, topics, abort)
	for val := range ch {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			break
		default:
			fmt.Printf("Get Value from channel %v", string(val))
		}
	}
	close(abort)
}
