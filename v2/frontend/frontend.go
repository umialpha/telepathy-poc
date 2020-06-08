package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
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
		}}
	bytes, err := proto.Marshal(value)
	if err != nil {
		return value, err
	}
	go s.kfclient.Produce(endQueueName(request.JobID), bytes)
	return value, nil

}

func (s *frontendServer) GetResponse(req *pb.JobRequest, stream pb.FrontendSvc_GetResponseServer) error {
	fmt.Println("GetResponse JobID: ", req.JobID)
	reqNum := req.ReqNum
	jobID := req.JobID
	abort := make(chan int)
	defer func() {
		abort <- 1
	}()
	ch, errCh := s.kfclient.Consume(endQueueName(jobID), jobID, abort)
	for i := int32(0); i < reqNum; i++ {
		select {
		case err := <-errCh:
			fmt.Println("Consume Backend Response Err: %v", err)
			return err
		case val := <-ch:
			resp := &pb.TaskResponse{}
			if err := proto.Unmarshal(val, resp); err != nil {
				fmt.Println("GetResp Unmarshel Error", err)
				continue
			}
			resp.Timestamp.Worker = time.Now().UnixNano()
			fmt.Println("Got Response TaskID, Resp Time ", resp.TaskID, resp.Timestamp)
			if err := stream.Send(resp); err != nil {
				fmt.Println("stream Send Err:%v", err)
				return err
			}
		}
	}

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
