package main

import (
	"flag"
	"fmt"
	"os"
	"time"
	"math/rand"
	"log"

	"google.golang.org/grpc/benchmark/stats"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func GetCPUTime() int64 {
	var ts unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_PROCESS_CPUTIME_ID, &ts); err != nil {
		fmt.Println(err)
		return 0
	}
	return ts.Nano()
}

func parseHist(hist *stats.Histogram) {
	fmt.Printf("Latency: (50/90/99 %%ile): %v/%v/%v\n",
		time.Duration(median(.5, hist)),
		time.Duration(median(.9, hist)),
		time.Duration(median(.99, hist)))
}

func median(percentile float64, h *stats.Histogram) int64 {
	need := int64(float64(h.Count) * percentile)
	have := int64(0)
	for _, bucket := range h.Buckets {
		count := bucket.Count
		if have+count >= need {
			percent := float64(need-have) / float64(count)
			return int64((1.0-percent)*bucket.LowBound + percent*bucket.LowBound*(1.0+hopts.GrowthFactor))
		}
		have += bucket.Count
	}
	panic("should have found a bound")
}

var hopts = stats.HistogramOptions{
	NumBuckets:   2495,
	GrowthFactor: .01,
}




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
	duration = flag.Int("d", 60, "duration in seconds")
	addr = flag.String("b", "bootstrap.kafka.svc.cluster.local:9092"  "broker address")
	topic = flag.String("t", "bench-test", "topic")
	msgSize = flag.Int("m", 1, "message size in bype")
	numMessages = flag.Int("n", 10000, "message count")

)


func produce(){
	value := make([]byte, &msgSize)
	rand.Read(value)
	var p, err = kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": &addr, "linger.ms": 100})
	if err != nil {
		log.Printf("could not set up kafka producer: %s", err.Error())
		os.Exit(1)
	}

	done := make(chan bool)
	go func() {
		var msgCount int
		for e := range p.Events() {
			msg := e.(*kafka.Message);
			if msg.TopicPartition.Error != nil {
				log.Printf("delivery report error: %v", msg.TopicPartition.Error)
				os.Exit(1)
			}
			msgCount++
			if msgCount >= &numMessages {
				done <- true
			}
		}
	}()

	defer p.Close()

	var start = time.Now()
	for j := 0; j < &numMessages; j++ {
		p.ProduceChannel() <- &kafka.Message{TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: int32(partition)}, Value: value}
	}
	<-done
	elapsed := time.Since(start)

	log.Printf("[confluent-kafka-go producer] msg/s: %f", (float64(&numMessages) / elapsed.Seconds()))
		



}


func consume(){
	var poll = true

	group, _ := newUUID()

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":               brokers,
		"group.id":                        group,
		"session.timeout.ms":              6000,
		"go.application.rebalance.enable": false,
		"enable.auto.commit":			   false,
		"auto.offset.reset": "earliest",
	})

	if err != nil {
		log.Printf("could not set up kafka consumer: %s", err.Error())
		os.Exit(1)
	}

	c.SubscribeTopics([]string{&topic}, nil)

	var start = time.Now()

	var msgCount = 0
	for msgCount < &numMessages {
		ev := c.Poll(100)
		if ev == nil {
			continue
		}
		switch e := ev.(type) {
		case *kafka.Message:
			msgCount++
			break
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

	elapsed := time.Since(start)

	log.Printf("[conflunet-kafka-go consumer] msg/s: %f", (float64(&numMessages) / elapsed.Seconds()))
}