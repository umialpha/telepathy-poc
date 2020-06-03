package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"telepathy.poc/mq"
	pb "telepathy.poc/protos"
)

var JOB_QUEUE string = "JOB_QUEUE"
var MQ_ADDR = flag.String("MQ_ADDR", "0.0.0.0:9092", "MQ ADDR")
var WORKER_LIST = flag.String("WORKER_LIST", "0.0.0.0:4002", "Worker list")

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

	for {
		select {
		case err := <-errCh:
			fmt.Println("Consume Task Queue Error", err)
			continue
		case val := <-ch:
			taskID, _ := strconv.Atoi(string(val))
			fmt.Println("Get Task %d", taskID)
			go s.dispatchTask(jobID, int32(taskID))

		}
	}

}

func (s *BackendServer) dispatchTask(jobID string, taskID int32) {
	idx := rand.Intn(len(s.workers))
	fmt.Printlf("dispatchTask %v:%v to client %v %v\n", jobID, taskID, idx, s.workers[idx])
	go func(i int) {
		ctx := context.WithTimeout(context.BackGround(), 10*time.Second)
		defer ctx.cancel()
		_, err := s.workers[i].SendTask(context.Background(), &pb.TaskRequest{JobID: jobID, TaskID: taskID})
		if err != nil {
			fmt.Println("dispatchTask %v:%v to client %v %v error: %v\n", jobID, taskID, i, s.workers[i], error)
		}
	}(idx)

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
