package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"t.poc.v3/mq"
	pb "t.poc.v3/protos"
)

var (
	qAddr         = flag.String("q", "0.0.0.0:9092", "MQ ADDR")
	workerAddrFmt = flag.String("w", "localhost:4002", "Worker Addr Format")
	workerNum     = flag.Int("n", 5, "worker number")
	jobQueue      = flag.String("j", "JOB-QUEUE-V3", "Job Queue")
)

type BackendServer struct {
	workers  []pb.WorkerSvcClient
	kfclient *mq.KafkaClient
}

func (s *BackendServer) run() {
	abortCh := make(chan int)
	defer func() {
		abortCh <- 1
	}()
	writeChan := make(chan []byte, 1000)
	errorCh := make(chan error, 1000)

	go s.kfclient.Consume(*jobQueue, fmt.Sprintf("%s", rand.Int()), writeChan, errorCh, abortCh)

	fmt.Println("Start to Receive Job")

	for {
		select {
		case err := <-errorCh:
			fmt.Println("Consume Job Queue Error", err)
			continue
		case val := <-writeChan:
			jobID := string(val)
			go s.startJob(jobID)

		}
	}

}

func (s *BackendServer) runOneQueue() {
	fmt.Println("test run one queue")
	s.startJob()
}

func (s *BackendServer) startJob(jobID string) {
	fmt.Println("startJob", jobID)
	abortCh := make(chan int)
	defer func() {
		abortCh <- 1
	}()
	writeCh := make(chan []byte, 1000)
	errorCh := make(chan error, 1000)
	go s.kfclient.Consume(jobID, jobID, writeCh, errorCh, abortCh)

	for {
		select {
		case err := <-errorCh:
			fmt.Println("Consume Task Queue Error", err)
			continue
		case val := <-writeCh:
			taskReq := &pb.TaskRequest{}
			if err := proto.Unmarshal(val, taskReq); err != nil {
				fmt.Println("Error To Unmarshal task", err)
				continue
			}
			//fmt.Println("Get Task %d", taskResp.TaskID)
			go s.dispatchTask(jobID, taskReq)

		}
	}

}

func (s *BackendServer) dispatchTask(jobID string, taskReq *pb.TaskRequest) {
	idx := rand.Intn(len(s.workers))
	//fmt.Printf("dispatchTask %v to client %v %v\n", taskReq, idx, s.workers[idx])
	go func(i int) {
		// ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		//defer cancel()
		_, err := s.workers[i].SendTask(context.Background(), &pb.TaskRequest{
			JobID:  jobID,
			TaskID: taskReq.TaskID,
			Timestamp: &pb.ModifiedTime{
				Client: taskReq.Timestamp.Client,
				Front:  taskReq.Timestamp.Front,
				Back:   time.Now().UnixNano(),
			}})
		if err != nil {
			fmt.Println("dispatchTask %v:%v to client %v %v error: %v\n", jobID, taskReq.TaskID, i, s.workers[i], err)
		}
	}(idx)

}

func NewBackendServer() *BackendServer {

	s := &BackendServer{}
	c, err := mq.NewKafkaClient(*qAddr)
	if err != nil {
		log.Fatalf("fail to create kafka client")
		return nil
	}
	s.kfclient = c

	for i := 0; i < *workerNum; i++ {
		var addr string
		if !strings.Contains(*workerAddrFmt, "localhost") {
			addr = fmt.Sprintf(*workerAddrFmt, i)

		} else {
			addr = *workerAddrFmt
		}
		fmt.Println("Worker Addr", addr)
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
	fmt.Println("flags", *qAddr, *workerAddrFmt, *workerNum)
	s := NewBackendServer()
	s.run()
}
