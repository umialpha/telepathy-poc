package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"gonum.org/v1/gonum/stat/distuv"
	klogr "k8s.io/klog/v2/klogr"
)

var (
	et         = flag.Int64("et", int64(1000), "estimated work time in ms")
	pulsarAddr = flag.String("pulsar", "pulsar://localhost:6650", "pulsar addr")
	qn         = flag.String("qn", "", "queue name")
	parallism  = flag.Int("p", 1, "parallsim worker process")
)

var logger = klogr.New().WithName("test-client")

type SafePoisson struct {
	mu sync.Mutex
	pn *distuv.Poisson
}

func (s *SafePoisson) Rand() float64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.pn.Rand()
}

func NewSafePoission(lambda float64) *SafePoisson {
	return &SafePoisson{
		pn: &distuv.Poisson{
			Lambda: lambda,
		},
	}
}

func process(consumer pulsar.Consumer, respProducer pulsar.Producer, metricProducer pulsar.Producer, pn *SafePoisson, finCh chan bool) {

	prodFunc := func(p pulsar.Producer, msg *pulsar.ProducerMessage) {

		callback := func(msgID pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
			// if produce failed, simply resend
			if err != nil {
				logger.Error(err, "Send Metric failed", "message", message)
				time.Sleep(time.Millisecond * 100)
				p.SendAsync(context.Background(), msg, nil)
			}
		}
		p.SendAsync(context.Background(), msg, callback)
	}

	for {
		select {
		case msg := <-consumer.Chan():
			d := int64(pn.Rand())
			logger.Info("Start processing, ", "It will take(MS)", d)
			time.Sleep(time.Millisecond * time.Duration(d))
			consumer.Ack(msg)
			prodFunc(respProducer, &pulsar.ProducerMessage{Payload: msg.Payload()})
			prodFunc(metricProducer, &pulsar.ProducerMessage{Payload: []byte(fmt.Sprintf("%d", d))})
		case <-finCh:
			logger.Info("process finished")
			return
		}
	}
}

func main() {
	flag.Parse()
	if *qn == "" {
		logger.Error(errors.New("queue name not set"), "queue name not set")
		return
	}
	pn := NewSafePoission(float64(*et))
	finCh := make(chan bool)
	// initialize producer and consumer
	client, err := pulsar.NewClient(pulsar.ClientOptions{URL: *pulsarAddr})
	if err != nil {
		logger.Error(err, "Initialize pulsar client failed")
		return
	}
	defer client.Close()

	respProducer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: *qn + "-resp", // resp queue
	})
	if err != nil {
		logger.Error(err, "Initialize pulsar producer failed")
		return
	}
	defer respProducer.Close()

	metricProducer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: *qn + "-metric", // metric queue
	})
	if err != nil {
		logger.Error(err, "Initialize pulsar producer failed")
		return
	}
	defer metricProducer.Close()

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:                       *qn,
		SubscriptionName:            *qn,
		Type:                        pulsar.Shared,
		SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
		ReceiverQueueSize:           *parallism,
		MessageChannel:              make(chan pulsar.ConsumerMessage, *parallism),
	})
	if err != nil {
		logger.Error(err, "Initialize pulsar consumer failed")
		return
	}
	defer consumer.Close()
	for i := 0; i < *parallism; i++ {
		go process(consumer, respProducer, metricProducer, pn, finCh)
	}

	//TODO when to finish?
	<-finCh

}
