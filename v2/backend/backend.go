package main

import (
	"fmt"
	"log"
	"os"
	"strings"
	"math/rand"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"google.golang.org/grpc"
	pb "telepathy.poc/protos"
)

const var JOB_QUEUE string = "JOB_QUEUE"

type BackendServer struct {
	workers  *[]pb.WorkerSvcClient
	kfclient interface{}
}

func (s *BackendServer) run() {
	abort := make(chan int)
	defer func() {
		abort <- 1
	}()
	ch, err := s.kfclient.Consume(JOB_QUEUE, abort)
	for val := range ch {
		jobID := val.(string)
		s.startJob(jobID)
	}
}

func (s *BackendServer) startJob(jobID string) {
	abort := make(chan int)
	defer func() {
		abort <- 1
	}()
	ch, err := s.kfclient.Consume(jobID, abort)
	for val := range ch {
		taskID := val.(int32)
		go s.dispatchTask(jobID, taskID)
	}
}

func (s *BackendServer) dispatchTask(jobID string, taskID int32) {
	idx := rand.Intn(len(workers))
	go workers[idx].SendTask(&pb.TaskRequest{JobID:jobID, TaskID: taskID})
}

func NewBackendServer() *BackendServer {

	broker := os.Getenv("MQ_ADDR")
	s := &BackendServer{}
	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":     broker,
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
