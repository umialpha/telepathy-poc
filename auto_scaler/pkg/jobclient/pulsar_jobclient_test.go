package jobclient

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

func TestCollectorCollectMetricForJob(t *testing.T) {
	os.Setenv(PULSAR_ADDR, "pulsar://40.119.250.46:6650")
	jobid := fmt.Sprintf("test-job-%04d", time.Now().Second())
	stopCh := make(chan bool)
	c := newJobCollector(jobid, stopCh)
	assert.Equal(t, int32(0), c.metric.SumTime)
	assert.Equal(t, int32(0), c.metric.Cnt)

	// produce some message

	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: os.Getenv(PULSAR_ADDR),
	})

	if err != nil {
		log.Fatal(err)
	}

	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: MetricQueueName(jobid),
	})
	if err != nil {
		log.Fatal(err)
	}

	defer producer.Close()

	mockExeTime := []int32{100, 200, 300, 400}
	sumTime := int32(0)
	for i := range mockExeTime {
		producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte(fmt.Sprintf("%d", mockExeTime[i])),
		})
		sumTime += mockExeTime[i]
		time.Sleep(time.Second)
		assert.Equal(t, sumTime, c.metric.SumTime)
		assert.Equal(t, int32(i+1), c.metric.Cnt)
	}
	stopCh <- true

}

func TestGetAverageExeutionMS(t *testing.T) {
	os.Setenv(PULSAR_ADDR, "pulsar://40.119.250.46:6650")
	jobid := fmt.Sprintf("test-job-%d", time.Now().Second())
	c := NewPulsarJobClient()
	avg := c.GetAverageExeutionMS(jobid)
	assert.Equal(t, int32(-1), avg)

	// initialize pulsar client
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: os.Getenv(PULSAR_ADDR),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: MetricQueueName(jobid),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	mockExeTime := []int32{100, 200, 300, 400}
	sumTime := int32(0)
	for i := range mockExeTime {
		producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte(fmt.Sprintf("%d", mockExeTime[i])),
		})
		sumTime += mockExeTime[i]
		time.Sleep(time.Second)
		avgExe := c.GetAverageExeutionMS(jobid)
		assert.Equal(t, int32(float32(sumTime)/(float32(i+1)+0.01)), avgExe)

	}

}
