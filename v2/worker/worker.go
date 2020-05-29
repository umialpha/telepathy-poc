package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"

	"google.golang.org/grpc"
	"telepathy.poc/mq"
	pb "telepathy.poc/protos"
)

func endQueueName(que string) string {
	return que + "-END"
}

type WorkerServer struct {
	pb.UnimplementedWorkerSvcServer
	kfclient mq.IQueueClient
}

func (w *WorkerServer) SendTask(ctx context.Context, req *pb.TaskRequest) (*pb.TaskResponse, error) {
	go w.doWork(req)
	return &pb.TaskResponse{JobID: req.JobID, TaskID: req.TaskID}, nil
}

func (w *WorkerServer) doWork(req *pb.TaskRequest) {
	w.kfclient.Produce(endQueueName(req.JobID), nil, &req.TaskID)
}

func newServer() *WorkerServer {
	mqAddr := os.Getenv("MQ_ADDR")
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
	grpcServer := grpc.NewServer()
	pb.RegisterWorkerSvcServer(grpcServer, newServer())
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%s", os.Getenv("PORT")))
	if err != nil {
		fmt.Println("Failed to Start Server %v", err)
		return
	}
	grpcServer.Serve(lis)

}
