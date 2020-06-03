package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"telepathy.poc/mq"
	pb "telepathy.poc/protos"
)

type TClient struct {
	serverAddr string
	client     pb.FrontendSvcClient
}

func (c *TClient) CreateJob(jobID string, reqNum int32) error {
	_, err := c.client.CreateJob(context.Background(), &pb.JobRequest{JobID: jobID, ReqNum: reqNum})
	if err != nil {
		fmt.Println("Create Job Err", err)
		return err
	}
	return nil
}

func (c *TClient) SendTask(jobID string, taskID int) error {
	_, err := c.client.SendTask(context.Background(), &pb.TaskRequest{JobID: jobID, TaskID: int32(taskID)})
	if err != nil {
		fmt.Println("Send Task Err", err)
		return err
	}
	return nil
}

func (c *TClient) GetResponse(jobID string, reqNum int32) chan int {
	ch := make(chan int, 10)

	stream, err := c.client.GetResponse(context.Background(), &pb.JobRequest{JobID: jobID, ReqNum: reqNum})
	if err != nil {
		log.Fatalf("GetResponse error %v.\n", err)
		close(ch)
		return ch
	}
	go func() {

		for {
			resp, err := stream.Recv()
			if err == io.EOF {
				close(ch)
				break
			}
			if err != nil {
				fmt.Println("GetStream err: ", err)
				close(ch)
				break
			}
			ch <- int(resp.TaskID)

		}
	}()
	return ch
}

func (c *TClient) CloseJob(jobID string) {

}

func NewTClient(addr string) *TClient {
	c := &TClient{
		serverAddr: addr,
	}

	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
		return nil
	}
	client := pb.NewFrontendSvcClient(conn)
	c.client = client
	return c
}

var FRONT_ADDR = flag.String("FRONT_ADDR", "localhost", "frontend server ip:port")
var REQ_NUM = flag.Int("REQ_NUM", 1, "requeset num")
var JOB_ID = flag.String("JOB_ID", "JOB-0", "JOB ID")
var c = flag.String("c", "1", "client side")
var MQ_ADDR = flag.String("MQ_ADDR", "0.0.0.0:9092", "MQ ADDR")
var PORT = flag.String("PORT", "6001", "server port")
var serverNum = flag.Int("serverNum", 16, "server num")

func Client() {
	startTime := time.Now()
	addr := *FRONT_ADDR
	request := *REQ_NUM
	jobID := *JOB_ID
	fmt.Println("Flags:", addr, request, jobID, *serverNum)
	var clients []*TClient
	for i := 0; i < *serverNum; i++ {
		caddr := fmt.Sprintf(addr, i)
		fmt.Println("Server Addr", caddr)
		clients = append(clients, NewTClient(caddr))
	}
	var wt sync.WaitGroup
	wt.Add(request)
	for t := 0; t < request; t++ {
		go func(i int) {
			defer wt.Done()
			clients[t%*serverNum].SendTask(jobID, t)
		}(t)

	}
	wt.Wait()
	end := time.Now()
	fmt.Println("ALL Cost", end.Sub(startTime))
	fmt.Println("throughput", int64(request)/(end.Sub(startTime).Milliseconds())*1000)
}

type frontendServer struct {
	pb.UnimplementedFrontendSvcServer
	kfclient mq.IQueueClient
}

func (s *frontendServer) CreateJob(ctx context.Context, request *pb.JobRequest) (*pb.JobResponse, error) {
	return &pb.JobResponse{JobID: request.JobID}, nil
}

func (s *frontendServer) SendTask(ctx context.Context, request *pb.TaskRequest) (*pb.TaskResponse, error) {

	return &pb.TaskResponse{JobID: request.JobID, TaskID: request.TaskID}, nil

}

func (s *frontendServer) GetResponse(req *pb.JobRequest, stream pb.FrontendSvc_GetResponseServer) error {
	return nil
}

func (s *frontendServer) CloseJob(context.Context, *pb.JobRequest) (*pb.JobResponse, error) {
	return nil, nil
}

func newServer() pb.FrontendSvcServer {
	s := &frontendServer{}
	return s
}

func Server() {
	fmt.Println("flags:", *MQ_ADDR, *PORT)
	grpcServer := grpc.NewServer()
	pb.RegisterFrontendSvcServer(grpcServer, newServer())
	lis, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%s", *PORT))
	if err != nil {
		fmt.Println("Failed to Start Server %v", err)
		return
	}
	grpcServer.Serve(lis)
}

func main() {
	flag.Parse()
	if *c == "1" {
		Client()
	} else {
		Server()
	}
	// fmt.Println("")
}
