package main

import (
	"context"
	"flag"
	"fmt"
	"net"
	"time"

	"google.golang.org/grpc"
	"t.poc.v4/metric"
	pb "t.poc.v4/protos"
)

var (
	msgNum     = flag.Int("n", 1000000, "msg number")
	port       = flag.String("p", "4002", "server port")
	testName   = flag.String("test_name", "", "Name of the test used for creating profiles.")
	cpuTick    = flag.Int("cpu_tick", 0, "if set, it will sampling cpu usage in 'cpu_tick' millisecond")
	warmUpTime = flag.Int("w", 120, "warmup time")
)

var startTime time.Time

type dispatcher struct {
	pb.DispatcherSvrServer
	msgChan  chan int
	respChan chan int
	ben      *metric.Bench
}

func (d *dispatcher) ReqTask(ctx context.Context, req *pb.TaskRequest) (*pb.TaskResponse, error) {
	if time.Now().Before(startTime) {
		// task 0 means it is still warm-up time
		return &pb.TaskResponse{TaskId: 0}, nil
	}
	if task, ok := <-d.msgChan; ok {
		d.ben.Start()
		return &pb.TaskResponse{TaskId: int32(task)}, nil
	} else {

		return &pb.TaskResponse{TaskId: int32(-1)}, nil
	}

}

func (d *dispatcher) FinTask(ctx context.Context, req *pb.TaskRequest) (*pb.TaskResponse, error) {
	d.respChan <- 1
	if len(d.respChan) == *msgNum {
		d.ben.End()
		fmt.Printf("duration: %v, qps: %.4f msg/s \n", d.ben.Duration(), float64(*msgNum)/d.ben.Duration().Seconds())
		d.ben.CPUUsage()
		//d.Restart()

	}
	return &pb.TaskResponse{TaskId: req.TaskId}, nil
}

func (d *dispatcher) fetcher() {
	defer close(d.msgChan)
	for i := 0; i < *msgNum; i++ {
		d.msgChan <- (i + 1)
	}
}

func (d *dispatcher) Restart() {
	fmt.Println("Restart")
	d.msgChan = make(chan int, *msgNum)
	d.respChan = make(chan int, *msgNum+10)
	d.ben = metric.NewBench(*testName, *cpuTick)
	go d.fetcher()
}

func NewDispatcherSvc() pb.DispatcherSvrServer {
	d := &dispatcher{
		msgChan:  make(chan int, *msgNum),
		respChan: make(chan int, *msgNum+10),
		ben:      metric.NewBench(*testName, *cpuTick),
	}
	go d.fetcher()
	return d
}

func main() {
	flag.Parse()
	grpcServer := grpc.NewServer()
	pb.RegisterDispatcherSvrServer(grpcServer, NewDispatcherSvc())
	lis, err := net.Listen("tcp", ":"+*port)
	if err != nil {
		fmt.Println("Failed to Start Server", err)
		return
	}
	startTime = time.Now().Add(time.Duration(*warmUpTime) * time.Second)
	grpcServer.Serve(lis)
}
