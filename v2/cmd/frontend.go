package main

import (
	"context"
	"fmt"
	"os"

	"google.golang.org/grpc"
	"telepathy.poc/mq"
	pb "telepathy.poc/protos"
)

func endQueueName(que string) string {
	return que + "-END"
}

var JOB_QUEUE string = "JOB_QUEUE"

type frontendServer struct {
	pb.UnimplementedFrontendSvcServer
	kfclient mq.IQueueClient
}

func (s *frontendServer) CreateJob(ctx context.Context, request *pb.JobRequest) (*pb.JobResponse, error) {

	errch := make(chan error)
	go func() {
		err := s.kfclient.CreateQueue(request.JobID)
		errch <- err
	}()
	err := s.kfclient.CreateQueue(endQueueName(request.JobID))
	if err != nil {
		return nil, err
	}
	err = <-errch
	if err != nil {
		return nil, err
	}
	go s.kfclient.Produce(JOB_QUEUE, nil, &request.JobID)
	return &pb.JobResponse{JobID: request.JobID}, nil
}

func (s *frontendServer) SendTask(ctx context.Context, request *pb.TaskRequest) (*pb.TaskResponse, error) {

	go s.kfclient.Produce(request.JobID, nil, &request.TaskID)

	return &pb.TaskResponse{JobID: request.JobID, TaskID: request.TaskID}, nil

}

func (s *frontendServer) GetResponse(req *pb.JobRequest, stream pb.FrontendSvc_GetResponseServer) error {
	reqNum := req.ReqNum
	jobID := req.JobID
	abort := make(chan int)
	defer func() {
		abort <- 1
	}()
	ch, err := s.kfclient.Consume(endQueueName(jobID), abort)
	if err != nil {
		return err
	}
	for i := int32(0); i < reqNum; i++ {
		val := <-ch
		v := val.(int32)
		resp := &pb.TaskResponse{JobID: jobID, TaskID: v}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}

	return nil
}

func (s *frontendServer) CloseJob(context.Context, *pb.JobRequest) (*pb.JobResponse, error) {
	return nil, nil
}

func newServer() pb.FrontendSvcServer {
	s := &frontendServer{}
	mqAddr := os.Getenv("MQ_ADDR")
	fmt.Println("Get MQ Addr %v", mqAddr)
	var err error
	s.kfclient, err = mq.NewKafkaClient(mqAddr)
	if err != nil {
		panic(err)
	}
	return s
}

func main() {
	grpcServer := grpc.NewServer()
	pb.RegisterFrontendSvcServer(grpcServer, newServer())
}
