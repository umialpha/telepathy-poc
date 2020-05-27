package main

import (
	"context"
	"log"
	"math/rand"
	"os"
	"strings"

	"google.golang.org/grpc"
	"telepathy.poc/mq"
	pb "telepathy.poc/protos"
)

var JOB_QUEUE string = "JOB_QUEUE"

type BackendServer struct {
	workers  []pb.WorkerSvcClient
	kfclient mq.IQueueClient
}

func (s *BackendServer) run() {
	abort := make(chan int)
	defer func() {
		abort <- 1
	}()
	ch, err := s.kfclient.Consume(JOB_QUEUE, abort)
	if err != nil {
		return
	}
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
	if err != nil {
		log.Fatalf("StartJob err %v.\n", err)
		return
	}
	for val := range ch {
		taskID := val.(int32)
		go s.dispatchTask(jobID, taskID)
	}
}

func (s *BackendServer) dispatchTask(jobID string, taskID int32) {
	idx := rand.Intn(len(s.workers))
	go s.workers[idx].SendTask(context.Background(), &pb.TaskRequest{JobID: jobID, TaskID: taskID})
}

func NewBackendServer() *BackendServer {

	broker := os.Getenv("MQ_ADDR")
	s := &BackendServer{}
	c, err := mq.NewKafkaClient(broker)
	if err != nil {
		log.Fatalf("fail to create kafka client")
		return nil
	}
	s.kfclient = c
	workerAddrs := os.Getenv("WORKER_LIST")
	workerList := strings.Split(workerAddrs, " ")
	for _, addr := range workerList {
		conn, err := grpc.Dial(addr)
		if err != nil {
			log.Fatalf("fail to dial: %v", err)
			conn.Close()
			continue
		}
		client := pb.NewWorkerSvcClient(conn)
		s.workers = append(s.workers, client)
	}
	return s
}

func main() {
	s := NewBackendServer()
	go s.run()
	forever := make(chan int)
	<-forever
}
