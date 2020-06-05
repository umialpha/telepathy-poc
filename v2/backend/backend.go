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
	"telepathy.poc/mq"
	pb "telepathy.poc/protos"
)

var (
	JOB_QUEUE     string = "JOB_QUEUE"
	qAddr                = flag.String("q", "0.0.0.0:9092", "MQ ADDR")
	workerAddrFmt        = flag.String("w", "localhost:4002", "Worker Addr Format")
	workerNum            = flag.Int("n", 5, "worker number")
)

type BackendServer struct {
	workers  []pb.WorkerSvcClient
	kfclient mq.IQueueClient
}

func (s *BackendServer) run() {
	abort := make(chan int)
	defer func() {
		abort <- 1
	}()
	ch, errCh := s.kfclient.Consume(JOB_QUEUE, fmt.Sprintf("%s", rand.Int()), abort)

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
	ch, errCh := s.kfclient.Consume(jobID, jobID, abort)

	for {
		select {
		case err := <-errCh:
			fmt.Println("Consume Task Queue Error", err)
			continue
		case val := <-ch:
			taskResp := &pb.TaskResponse{}
			if err := proto.Unmarshal(val, taskResp); err != nil {
				fmt.Println("Error To Unmarshal task", err)
				continue
			}
			fmt.Println("Get Task %d", taskResp.TaskID)
			go s.dispatchTask(jobID, taskResp)

		}
	}

}

func (s *BackendServer) dispatchTask(jobID string, taskResp *pb.TaskResponse) {
	idx := rand.Intn(len(s.workers))
	fmt.Printf("dispatchTask %v:%v to client %v %v\n", jobID, taskResp.TaskID, idx, s.workers[idx])
	go func(i int) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := s.workers[i].SendTask(ctx, &pb.TaskRequest{
			JobID:  jobID,
			TaskID: taskResp.TaskID,
			Timestamp: &pb.ModifiedTime{
				Client: taskResp.Timestamp.Client,
				Front:  taskResp.Timestamp.Front,
				Back:   time.Now().UnixNano(),
			}})
		if err != nil {
			fmt.Println("dispatchTask %v:%v to client %v %v error: %v\n", jobID, taskResp.TaskID, i, s.workers[i], err)
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
