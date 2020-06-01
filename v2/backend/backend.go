package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"strings"

	"google.golang.org/grpc"
	"telepathy.poc/mq"
	pb "telepathy.poc/protos"
)

var JOB_QUEUE string = "JOB_QUEUE"
var MQ_ADDR = flag.String("MQ_ADDR", "localhost:9092", "MQ ADDR")
var WORKER_LIST = flag.String("WORKER_LIST", "localhost:4002", "Woerker list")

type BackendServer struct {
	workers  []pb.WorkerSvcClient
	kfclient mq.IQueueClient
}

func (s *BackendServer) run() {
	abort := make(chan int)
	defer func() {
		abort <- 1
	}()
	ch, errCh := s.kfclient.Consume(JOB_QUEUE, abort)

	fmt.Println("Start to Receive Job")

	for {
		select {
		case err := <-errCh:
			fmt.Println("Consume Job Queue Error", err)
			continue
		case val := <-ch:
			jobID := string(val)
			fmt.Println("Got Job", jobID)
			go s.startJob(jobID)

		}
	}

}

func (s *BackendServer) startJob(jobID string) {
	fmt.Println("startJob", jobID)
	abort := make(chan int)
	defer func() {
		abort <- 1
	}()
	ch, errCh := s.kfclient.Consume(jobID, abort)

	for val := range ch {
		err := <-errCh
		if err != nil {
			log.Fatalf("StartJob err %v.\n", err)
			return
		}
		taskID, _ := strconv.Atoi(string(val))
		fmt.Println("Get Task %d", taskID)
		go s.dispatchTask(jobID, int32(taskID))
	}
}

func (s *BackendServer) dispatchTask(jobID string, taskID int32) {
	idx := rand.Intn(len(s.workers))
	go s.workers[idx].SendTask(context.Background(), &pb.TaskRequest{JobID: jobID, TaskID: taskID})
}

func NewBackendServer() *BackendServer {

	broker := *MQ_ADDR
	s := &BackendServer{}
	c, err := mq.NewKafkaClient(broker)
	if err != nil {
		log.Fatalf("fail to create kafka client")
		return nil
	}
	s.kfclient = c
	workerAddrs := *WORKER_LIST
	workerList := strings.Split(workerAddrs, " ")
	for _, addr := range workerList {
		fmt.Println("Worker ADDR %v", addr)
		conn, err := grpc.Dial(addr, grpc.WithInsecure())
		if err != nil {
			fmt.Println("fail to dial: %v", err)
			conn.Close()
			continue
		}
		client := pb.NewWorkerSvcClient(conn)
		s.workers = append(s.workers, client)
	}
	return s
}

func main() {
	flag.Parse()
	s := NewBackendServer()
	s.run()
}
