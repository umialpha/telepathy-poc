package queueclient

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/stretchr/testify/assert"

	. "poc.telepathy.scale/pkg/config"
)

func TestIsQueueExist(t *testing.T) {
	os.Setenv(PULSAR_ADDR, "pulsar://40.119.250.46:6650")
	os.Setenv(PULSAR_ADMIN_ADDR, "http://40.119.250.46")
	jobid := fmt.Sprintf("test-job-%04d", time.Now().Second())
	log.Println("jobid", jobid)
	c := NewPulsarQueueClient()
	assert.False(t, c.IsQueueExist(jobid))

	// initialize pulsar client
	// and produce some messages

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: os.Getenv(PULSAR_ADDR),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: jobid,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	producer.Send(context.Background(), &pulsar.ProducerMessage{
		Payload: []byte(fmt.Sprintf("hello")),
	})
	assert.True(t, c.IsQueueExist(jobid))

}

func TestGetQueueLength(t *testing.T) {
	os.Setenv(PULSAR_ADDR, "pulsar://40.119.250.46:6650")
	os.Setenv(PULSAR_ADMIN_ADDR, "http://40.119.250.46")
	jobid := fmt.Sprintf("test-job-%d", time.Now().Second())
	c := NewPulsarQueueClient()

	// initialize pulsar client
	// and produce some messages
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: os.Getenv(PULSAR_ADDR),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: jobid,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()

	messageLen := 10

	for i := 1; i <= messageLen; i++ {
		producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello")),
		})
		assert.Equal(t, int32(i), c.GetQueueLength(jobid))
	}

	// initialze pulsar consumer
	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       jobid,
		SubscriptionName:            jobid,
		Type:                        pulsar.Shared,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer consumer.Close()
	for i := 1; i <= 10; i++ {
		msg, err := consumer.Receive(context.Background())
		logger.Info("Get a message", string(msg.Payload()))
		if err != nil {
			log.Fatal(err)
		}
		consumer.Ack(msg)
		time.Sleep(time.Second)
		assert.Equal(t, int32(messageLen-i), c.GetQueueLength(jobid))
	}

}
