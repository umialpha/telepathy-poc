package main

import (
	"context"
	"os"
	

	"google.golang.org/grpc"
	"telepathy.poc/mq"
	pb "telepathy.poc/protos"
)

func endQueaueName(que string) string {
	return que + "-END"
}

var JOB_QUEUE string = "JOB_QUEUE"

type frontendServer struct {
	pb.UnimplementedFrontendSvcServer
	kfclient *mq.IQueueClient
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
	go s.kfclient.Produce(JOB_QUEUE, request.JobID)
	return &pb.JobResponse{JobID: request.JobId}, nil
}

func (s *frontendServer) SendTask(ctx context.Context, request *pb.TaskRequest) (*pb.TaskResponse, error) {

	go s.kfclient.Produce(request.JobID, request.TaskID)

	return &pb.TaskResponse{JobID: request.JobID, Taskid: request.TaskID}, nil

}

func (s *frontendServer) GetResponse(req *JobRequest, stream FrontendSvc_GetResponseServer) error {
	reqNum := req.ReqNum
	jobID := req.JobID
	abort := make(chan int)
	defer func() {
		abort <- 1
	}
	ch, err := s.kfclient.Consume(endQueaueName(jobID), abort)
	if err != nil {
		return err
	}
	for i := 0; i < reqNum; i++ {
		val := <- ch
		val = val.(int32)
		resp := pb.TaskResponse{JobId:jobID, TaskID:value}
		if err := stream.Send(resp); err != nil {
			return err
		}
	}
	
}

func (s *frontendServer) CloseJob(context.Context, *JobRequest) (*JobResponse, error) {
	return nil, nil
}

func newServer() *pb.FrontendSvcServer {
	s := &frontendServer{}
	mqAddr := os.Getenv("MQ_ADDR")
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
