package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
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
	return &pb.TaskResponse{
		JobID:  req.JobID,
		TaskID: req.TaskID}, nil
}

func (w *WorkerServer) doWork(req *pb.TaskRequest) {
	value := &pb.TaskResponse{
		JobID:  req.JobID,
		TaskID: req.TaskID,
		Timestamp: &pb.ModifiedTime{
			Client: req.Timestamp.Client,
			Front:  req.Timestamp.Front,
			Back:   req.Timestamp.Back,
			Worker: req.Timestamp.Worker,
		}}
	if bytes, err := proto.Marshal(value); err != nil {
		fmt.Println("DoWork Marshal Error", err)
		return
	}
	w.kfclient.Produce(endQueueName(req.JobID), bytes)
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
	fmt.Println("flags: PORT", *PORT)
	grpcServer := grpc.NewServer()
	pb.RegisterWorkerSvcServer(grpcServer, newServer())
	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", *PORT))
	if err != nil {
		fmt.Println("Failed to Start Server %v", err)
		return
	}
	grpcServer.Serve(lis)

}
