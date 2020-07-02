package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc"
	"telepathy.poc/mq"
	pb "telepathy.poc/protos"
)

func endQueueName(que string) string {
	return que + "-END"
}

type frontendServer struct {
	pb.UnimplementedFrontendSvcServer
	kfclient mq.IQueueClient
}

func (s *frontendServer) CreateJob(ctx context.Context, request *pb.JobRequest) (*pb.JobResponse, error) {

	fmt.Println("CreateJob JOB_ID: %v, REQ_NUM:%v", request.JobID, request.ReqNum)
	err := s.kfclient.CreateQueues([]string{*jobQueue, request.JobID, endQueueName(request.JobID)})
	if err != nil {
		fmt.Println("CreateQueue Error %v", err)
		return nil, err
	}
	go s.kfclient.Produce(*jobQueue, []byte(request.JobID))
	return &pb.JobResponse{
		JobID: request.JobID,
		Timestamp: &pb.ModifiedTime{
			Client: request.Timestamp.Client,
			Front:  time.Now().UnixNano(),
		}}, nil
}

func (s *frontendServer) SendTask(ctx context.Context, request *pb.TaskRequest) (*pb.TaskResponse, error) {
	fmt.Println("SendTask JobID TaskID: ", request.JobID, request.TaskID)

	value := &pb.TaskResponse{
		JobID:  request.JobID,
		TaskID: request.TaskID,
		Timestamp: &pb.ModifiedTime{
			Client: request.Timestamp.Client,
			Front:  time.Now().UnixNano(),
			Back:   time.Now().UnixNano(),
			Worker: time.Now().UnixNano(),
		}}

	return value, nil

}

func (s *frontendServer) GetResponse(req *pb.JobRequest, stream pb.FrontendSvc_GetResponseServer) error {
	return nil
}

func (s *frontendServer) CloseJob(ctx context.Context, req *pb.JobRequest) (*pb.JobResponse, error) {
	fmt.Println("CloseJob", req.JobID)
	err := s.kfclient.DeleteQueues([]string{req.JobID, endQueueName(req.JobID)})
	return &pb.JobResponse{JobID: req.JobID}, err
}

func newServer() pb.FrontendSvcServer {
	s := &frontendServer{}
	var err error
	s.kfclient, err = mq.NewKafkaClient(*qAddr)
	if err != nil {
		panic(err)
	}
	return s
}

var qAddr = flag.String("q", "0.0.0.0:9092", "MQ ADDR")
var port = flag.String("p", "4001", "server port")
var jobQueue = flag.String("j", "JOB-QUEUE", "Job Queue")

func main() {
	flag.Parse()
	fmt.Println(*qAddr, *port)
	grpcServer := grpc.NewServer()
	pb.RegisterFrontendSvcServer(grpcServer, newServer())
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", *port))
	if err != nil {
		fmt.Println("Failed to Start Server %v", err)
		return
	}
	grpcServer.Serve(lis)
}
