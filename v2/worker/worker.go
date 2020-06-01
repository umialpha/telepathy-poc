package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"
	"telepathy.poc/mq"
	pb "telepathy.poc/protos"
)

var MQADDR = flag.String("MQ_ADDR", "localhost:9092", "mq addr")
var PORT = flag.String("PORT", "4002", "server port")

func endQueueName(que string) string {
	return que + "-END"
}

type WorkerServer struct {
	pb.UnimplementedWorkerSvcServer
	kfclient mq.IQueueClient
}

func (w *WorkerServer) SendTask(ctx context.Context, req *pb.TaskRequest) (*pb.TaskResponse, error) {
	fmt.Println("SendTask", req.JobID, req.TaskID)
	go w.doWork(req)
	return &pb.TaskResponse{JobID: req.JobID, TaskID: req.TaskID}, nil
}

func (w *WorkerServer) doWork(req *pb.TaskRequest) {
	w.kfclient.Produce(endQueueName(req.JobID), nil, &req.TaskID)
}

func newServer() *WorkerServer {
	mqAddr := *MQADDR
	w := &WorkerServer{}
	c, err := mq.NewKafkaClient(mqAddr)
	if err != nil {
		log.Fatalf("Create Worker Kafka Client error: %v.\n", err)
		return nil
	}
	w.kfclient = c
	return w
}

func main() {
	flag.Parse()
	grpcServer := grpc.NewServer()
	pb.RegisterWorkerSvcServer(grpcServer, newServer())
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%s", *PORT))
	if err != nil {
		fmt.Println("Failed to Start Server %v", err)
		return
	}
	grpcServer.Serve(lis)

}
