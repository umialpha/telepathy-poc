package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"t.poc.v3/mq"
	pb "t.poc.v3/protos"
)

var qAddr = flag.String("q", "localhost:9092", "mq addr")
var port = flag.String("p", "4002", "server port")

func endQueueName(que string) string {
	return que + "-END"
}

type WorkerServer struct {
	pb.UnimplementedWorkerSvcServer
	kfclient *mq.KafkaClient
}

func (w *WorkerServer) SendTask(ctx context.Context, req *pb.TaskRequest) (*pb.TaskResponse, error) {

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
			Worker: time.Now().UnixNano(),
			End:    time.Now().UnixNano(),
		}}
	var bytes []byte
	var err error
	if bytes, err = proto.Marshal(value); err != nil {
		fmt.Println("DoWork Marshal Error", err)
		return
	}
	fmt.Println("DoWork", value)
	w.kfclient.Produce(endQueueName(req.JobID), bytes)
}

func newServer() *WorkerServer {
	w := &WorkerServer{}
	c, err := mq.NewKafkaClient(*qAddr)
	if err != nil {
		log.Fatalf("Create Worker Kafka Client error: %v.\n", err)
		return nil
	}
	w.kfclient = c
	return w
}

func main() {
	flag.Parse()
	fmt.Println("flags", *qAddr, *port)
	grpcServer := grpc.NewServer()
	pb.RegisterWorkerSvcServer(grpcServer, newServer())
	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", *port))
	if err != nil {
		fmt.Println("Failed to Start Server %v", err)
		return
	}
	grpcServer.Serve(lis)

}
