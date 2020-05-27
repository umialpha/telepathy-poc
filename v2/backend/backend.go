package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"math/rand"
	"context"
	"os/signal"

	//"github.com/confluentinc/confluent-kafka-go/kafka"
	"google.golang.org/grpc"
	pb "telepathy.poc/protos"
)

type BackendServer struct {
	consumer *kafka.Consumer
	workers  *[]pb.WorkerSvcClient
}

func (s *BackendServer) run() {
	err = s.consumer.SubscribeTopics(["SessionJob"], nil)
	for {
		ev := s.consumer.Poll(100)
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
			go s.StartJob(string(e.Value))
		case kafka.Error:
			// Errors should generally be considered
			// informational, the client will try to
			// automatically recover.
			// But in this example we choose to terminate
			// the application if all brokers are down.
			fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
			if e.Code() == kafka.ErrAllBrokersDown {
				run = false
			}
		default:
			fmt.Printf("Ignored %v\n", e)
	}
}


func (s *BackendServer) StartJob(jobID string) {
	broker := os.Getenv("MQ_ADDR")
	group := jobID
	topics := []string{jobID}
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": broker,
		"broker.address.family": "v4",
		"group.id":              group,
		"session.timeout.ms":    6000,
		"auto.offset.reset":     "earliest"})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Consumer %v\n", c)

	err = c.SubscribeTopics(topics, nil)

	run := true

	for run == true {
		select {
		case sig := <-sigchan:
			fmt.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			ev := c.Poll(100)
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
				wid := rand.Intn(len(s.workers))
				s.workers[wid].SendTask(context.Background(), &pb.BackendTaskRequest{TaskID: int32(e.Value)})
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}

	fmt.Printf("Closing consumer\n")
	c.Close()


}

func NewBackendServer() *BackendServer {

	broker := os.Getenv("MQ_ADDR")
	s := &BackendServer{}
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": broker,
		"broker.address.family": "v4",
		"group.id":              "GroupID",
		"session.timeout.ms":    6000,
		"auto.offset.reset":     "earliest"})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}
	s.consumer = c

	workerAddrs := os.Getenv("WORKER_LIST")

	workerList := strings.Split(workerAddrs, " ")
	for addr := range workerList {
		conn, err := grpc.Dial(addr)
		if err != nil {
			log.Fatalf("fail to dial: %v", err)
			conn.Close()
			continue
		}
		client := pb.NewWorkerSvcClient(conn)
		append(s.workers, client)
	}

	return s
}

func main() {

}
