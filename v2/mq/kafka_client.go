package mq

import (
	"context"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type kafkaClient struct {
	IQueueClient
	producer *kafka.Producer
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
			NumPartitions:     3,
			ReplicationFactor: 3}},

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
	bvalue, _ := value.([]byte)
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
		fmt.Printf("Delivered message to topic %s [%d] at offset %v\n",
			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	}
	return nil
}

func (c *kafkaClient) Consume(queueName string, opt ...interface{}) (interface{}, error) {
	return nil, nil
}

func NewKafkaClient(mqAddr string) (IQueueClient, error) {
	c := &kafkaClient{}
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": mqAddr})
	if err != nil {
		return nil, err
	}
	c.producer = p
	return c, nil
}
