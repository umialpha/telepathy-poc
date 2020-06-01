package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"strconv"

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

	fmt.Println("CreateJob JOB_ID: %v, REQ_NUM:%v", request.JobID, request.ReqNum)
	errch := make(chan error)
	go func() {
		err := s.kfclient.CreateQueue(request.JobID)
		errch <- err
	}()
	err := s.kfclient.CreateQueue(endQueueName(request.JobID))
	if err != nil {
		fmt.Println("CreateQueue Error %v", err)
		return nil, err
	}
	err = <-errch
	if err != nil {
		fmt.Println("CreateQueue Error %v", err)
		return nil, err
	}
	go s.kfclient.Produce(JOB_QUEUE, []byte(request.JobID))
	return &pb.JobResponse{JobID: request.JobID}, nil
}

func (s *frontendServer) SendTask(ctx context.Context, request *pb.TaskRequest) (*pb.TaskResponse, error) {
	fmt.Println("SendTask JobID TaskID: ", request.JobID, request.TaskID)
	go s.kfclient.Produce(request.JobID, []byte(fmt.Sprintf("%d", request.TaskID)))

	return &pb.TaskResponse{JobID: request.JobID, TaskID: request.TaskID}, nil

}

func (s *frontendServer) GetResponse(req *pb.JobRequest, stream pb.FrontendSvc_GetResponseServer) error {
	fmt.Println("GetResponse JobID: ", req.JobID)
	reqNum := req.ReqNum
	jobID := req.JobID
	abort := make(chan int)
	defer func() {
		abort <- 1
	}()
	ch, errCh := s.kfclient.Consume(endQueueName(jobID), abort)

	for i := int32(0); i < reqNum; i++ {
		err := <-errCh
		if err != nil {
			fmt.Println("Consume Backend Response Err: %v", err)
			return err
		}
		val := <-ch

		v := strconv.Atoi(string(val))
		fmt.Println("Got Value ", v)
		resp := &pb.TaskResponse{JobID: jobID, TaskID: v}
		if err := stream.Send(resp); err != nil {
			fmt.Println("stream Send Err:%v", err)
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
	mqAddr := *MQ_ADDR
	fmt.Println("Get MQ Addr %v", mqAddr)
	var err error
	s.kfclient, err = mq.NewKafkaClient(mqAddr)
	if err != nil {
		panic(err)
	}
	return s
}

var MQ_ADDR = flag.String("MQ_ADDR", "localhost:9092", "MQ ADDR")
var PORT = flag.String("PORT", "4001", "server port")

func main() {
	flag.Parse()
	grpcServer := grpc.NewServer()
	pb.RegisterFrontendSvcServer(grpcServer, newServer())
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%s", *PORT))
	if err != nil {
		fmt.Println("Failed to Start Server %v", err)
		return
	}
	grpcServer.Serve(lis)
}
