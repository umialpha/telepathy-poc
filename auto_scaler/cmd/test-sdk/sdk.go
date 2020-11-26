package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"k8s.io/klog/klogr"
)

// var (
// 	pulsarAddr = flag.String("pulsar", "pulsar://localhost:6650", "pulsar addr")
// 	qn         = flag.String("qn", "", "queue name")
// )

var logger = klogr.New().WithName("test-sdk")

func executeJob(addr string, qn string, taskNum int, deadline string) {

	client, err := pulsar.NewClient(pulsar.ClientOptions{URL: addr})
	if err != nil {
		logger.Error(err, "Initialize pulsar client failed")
		return
	}
	defer client.Close()
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic:           qn,
		DisableBatching: true,
	})
	if err != nil {
		logger.Error(err, "Initialize pulsar producer failed")
		return
	}
	defer producer.Close()
	reader, err := client.CreateReader(pulsar.ReaderOptions{
		Topic:          qn + "-resp",
		StartMessageID: pulsar.EarliestMessageID(),
	})
	if err != nil {
		logger.Error(err, "Init Pulsar reader error")
		return
	}

	resp := make(map[string]string)

	// produce messages
	go func() {
		callback := func(msgID pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
			// if produce failed, simply resend
			if err != nil {
				logger.Error(err, "Send msg failed", "message", message)
				// time.Sleep(time.Millisecond * 100)
				// producer.SendAsync(context.Background(), message, callback)
			}
		}

		for i := 0; i < taskNum; i++ {
			msg := &pulsar.ProducerMessage{
				Payload: []byte(fmt.Sprintf("%d", i))}
			producer.SendAsync(context.Background(), msg, callback)
		}

	}()

	for {
		msg, err := reader.Next(context.Background())
		if err != nil {
			logger.Error(err, "Consume Message Error")
			continue
		}
		logger.Info("Receive a message")
		taskID := string(msg.Payload())
		resp[taskID] = ""
		if len(resp) == taskNum {
			logger.Info("Job Finished!", "deadline", deadline, "now", time.Now().Format(time.RFC3339))
			return
		}

	}

}

func main() {
	flag.Parse()
	// t, _ := time.Parse(time.RFC3339, "2020-11-09T21:24:04+08:00")
	// fmt.Println(t.Sub(time.Now()))
	// fmt.Println(time.Now().Format(time.RFC3339))
	executeJob("pulsar://40.119.250.46:6650", "keda-test-queue-8", 30000, "2020-11-12T17:35:04+08:00")
}
