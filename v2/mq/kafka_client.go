package mq

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"os"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func GetBytes(key interface{}) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(key)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

type kafkaClient struct {
	IQueueClient
	producer   *kafka.Producer
	brokerAddr string
}

func (c *kafkaClient) CreateQueue(name string, opt ...interface{}) error {
	a, err := kafka.NewAdminClientFromProducer(c.producer)
	if err != nil {
		fmt.Printf("Failed to create new admin client from producer: %s", err)
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	results, err := a.CreateTopics(
		ctx,
		[]kafka.TopicSpecification{{
			Topic:             name,
			NumPartitions:     1,
			ReplicationFactor: 1}},

		kafka.SetAdminOperationTimeout(60*time.Second))
	if err != nil {
		fmt.Printf("Admin Client request error: %v\n", err)
		return err
	}
	for _, result := range results {
		if result.Error.Code() != kafka.ErrNoError && result.Error.Code() != kafka.ErrTopicAlreadyExists {
			fmt.Printf("Failed to create topic: %v\n", result.Error)
		}

		fmt.Printf("%v\n", result)
	}
	a.Close()
	return nil
}

func (c *kafkaClient) Produce(queueName string, key interface{}, value interface{}, opt ...interface{}) error {
	deliveryChan := make(chan kafka.Event)
	bvalue, err := GetBytes(value)
	if err != nil {
		fmt.Println("Cannot convert value to []byte ", err)
		return err
	}
	c.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &queueName, Partition: kafka.PartitionAny},
		Value:          bvalue,
	}, deliveryChan)
	e := <-deliveryChan
	m := e.(*kafka.Message)
	if m.TopicPartition.Error != nil {
		fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
		return m.TopicPartition.Error
	} else {
		fmt.Printf("Delivered message to topic %s, message: %s\n",
			*m.TopicPartition.Topic, m.Value)
	}
	return nil
}

func (c *kafkaClient) Consume(queueName string, abort <-chan int, opt ...interface{}) (<-chan interface{}, <-error) {
	ch := make(chan interface{}, 1000)
	errCh := error(nil)
	go func() {
		consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
			"bootstrap.servers":     c.brokerAddr,
			"broker.address.family": "v4",
			"group.id":              queueName,
			"session.timeout.ms":    6000,
			"auto.offset.reset":     "earliest"})
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
			ch <- nil, errCh <- err
			return
		}
		defer consumer.Close()
		err = consumer.SubscribeTopics([]string{queueName}, nil)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to SubscribeTopics : %s\n", queueName)
			ch <- nil, errCh <- err
			return
		}
		for {
			select {
			case <-abort:
				fmt.Printf("Caught abort")
				return
			default:
				ev := consumer.Poll(100)
				if ev == nil {
					continue
				}
				switch e := ev.(type) {
				case *kafka.Message:
					fmt.Printf("%% Message on %s: %s",
						e.TopicPartition, string(e.Value))
					if e.Headers != nil {
						fmt.Printf("%% Headers: %v\n", e.Headers)
					}
					ch <- e.Value
				}
			}
		}
	}()
	return ch, errCh
}

func NewKafkaClient(mqAddr string) (IQueueClient, error) {
	c := &kafkaClient{}
	c.brokerAddr = mqAddr
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": mqAddr})
	if err != nil {
		return nil, err
	}
	c.producer = p
	return c, nil
}
