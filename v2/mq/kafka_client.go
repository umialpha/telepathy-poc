package mq

import (
	"context"
	"fmt"
	"time"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type kafkaClient struct {
	IQueueClient
	producer *kafka.Producer
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

func (c *kafkaClient) Consume(queueName string, abort <-chan int, opt ...interface{}) (<-chan interface{}, error) {
	ch := make(chan interface{}, 1000)
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": c.brokerAddr,
		"broker.address.family": "v4",
		"group.id":              queueName,
		"session.timeout.ms":    6000,
		"auto.offset.reset":     "earliest"})
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		return nil, err
	}
	defer consumer.close()
	err = consumer.SubscribeTopics([queueName], nil)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to SubscribeTopics : %s\n", queueName)
		return nil, err
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
						fmt.Printf("%% Message on %s:\n%s\n",
							e.TopicPartition, string(e.Value))
						if e.Headers != nil {
							fmt.Printf("%% Headers: %v\n", e.Headers)
						}
						ch <- e.Value
				}
		}	
	}
	return ch, nil
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
