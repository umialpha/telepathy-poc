package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc"
	"t.poc.v3/mq"
	pb "t.poc.v3/protos"
)

var (
	qAddr    = flag.String("q", "0.0.0.0:9092", "MQ ADDR")
	port     = flag.String("p", "4001", "server port")
	jobQueue = flag.String("j", "JOB-QUEUE-V3", "Job Queue")
	testOne  = flag.Bool("testOne", false, "test one queue")
	jobQueue = flag.String("j", "JOB-QUEUE-V3-1", "Job Queue")
)

func endQueueName(que string) string {
	return que + "-END"
}

type frontendServer struct {
	pb.UnimplementedFrontendSvcServer
	kfclient *mq.KafkaClient
}

func (s *frontendServer) CreateJob(ctx context.Context, request *pb.JobRequest) (*pb.JobResponse, error) {

	fmt.Println("CreateJob JOB_ID: %v, REQ_NUM:%v", request.JobID, request.ReqNum)
	err := s.kfclient.CreateQueues([]string{*jobQueue, request.JobID, endQueueName(request.JobID)})
	if err != nil {
		fmt.Println("CreateQueue Error %v", err)
		return nil, err
	}
	s.kfclient.Produce(*jobQueue, []byte(request.JobID))
	return &pb.JobResponse{
		JobID: request.JobID,
		Timestamp: &pb.ModifiedTime{
			Client: request.Timestamp.Client,
			Front:  time.Now().UnixNano(),
		}}, nil
}

func (s *frontendServer) runOneQueue() {
	err := s.kfclient.CreateQueues([]string{*jobQueue, endQueueName(*jobQueue)})
	if err != nil {
		fmt.Println("CreateQueue Error %v", err)
		return
	}
}

func (s *frontendServer) SendTask(ctx context.Context, request *pb.TaskRequest) (*pb.TaskResponse, error) {
	return nil, nil

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
	if *testOne {
		// test read only one queue
		s.runOneQueue()
	}

	return s
}

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
