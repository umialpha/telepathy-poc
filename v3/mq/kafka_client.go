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

type KafkaClient struct {
	producer   *kafka.Producer
	brokerAddr string
}

func (c *KafkaClient) Producer() *kafka.Producer {
	return c.producer
}

func (c *KafkaClient) CreateQueues(names []string, opt ...interface{}) error {
	a, err := kafka.NewAdminClientFromProducer(c.producer)
	if err != nil {
		fmt.Printf("Failed to create new admin client from producer: %s", err)
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var topicSpecs []kafka.TopicSpecification
	for _, name := range names {
		topicSpecs = append(topicSpecs, kafka.TopicSpecification{
			Topic:             name,
			NumPartitions:     1,
			ReplicationFactor: 1,
		})
	}
	results, err := a.CreateTopics(
		ctx,
		topicSpecs,
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

func (c *KafkaClient) DeleteQueues(names []string, opt ...interface{}) error {
	a, err := kafka.NewAdminClientFromProducer(c.producer)
	if err != nil {
		fmt.Printf("Failed to create new admin client from producer: %s", err)
		return err
	}
	results, err := a.DeleteTopics(context.Background(), names, kafka.SetAdminOperationTimeout(time.Second*60))
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

func (c *KafkaClient) Produce(queueName string, value []byte, opt ...interface{}) error {
	c.producer.ProduceChannel() <- &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &queueName, Partition: kafka.PartitionAny},
		Value:          value,
	}

	return nil
}

func (c *KafkaClient) Consume(queueName string, groupID string, writeChan chan<- []byte, errCh chan<- error, abortCh <-chan int) {

	defer close(writeChan)
	defer close(errCh)
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":     c.brokerAddr,
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
				writeChan <- e.Value
			}
		}
	}

	return
}

func NewKafkaClient(mqAddr string) (*KafkaClient, error) {
	c := &KafkaClient{}
	c.brokerAddr = mqAddr
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": mqAddr /*"linger.ms": 100*/})
	if err != nil {
		return nil, err
	}
	c.producer = p
	return c, nil
}
