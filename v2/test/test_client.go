package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"sort"
	"time"

	"github.com/shirou/gopsutil/cpu"
	"google.golang.org/grpc"
	pb "telepathy.poc/protos"
)

type TClient struct {
	serverAddr string
	client     pb.FrontendSvcClient
}

func (c *TClient) CreateJob(jobID string, reqNum int32) (*pb.JobResponse, error) {
	return c.client.CreateJob(context.Background(), &pb.JobRequest{JobID: jobID, ReqNum: reqNum})
}

func (c *TClient) SendTask(jobID string, taskID int) (*pb.TaskResponse, error) {
	return c.client.SendTask(context.Background(), &pb.TaskRequest{JobID: jobID, TaskID: int32(taskID)})
}

func (c *TClient) GetResponse(jobID string) chan int {
	ch := make(chan int, 10)

	stream, err := c.client.GetResponse(context.Background(), &pb.JobRequest{JobID: jobID})
	if err != nil {
		log.Fatalf("GetResponse error %v.\n", err)
		close(ch)
		return ch
	}

	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			close(ch)
			break
		}
		ch <- int(resp.TaskID)

	}
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

func main() {
	addr := "20.43.88.175:4520"
	request := 2
	jobID := "test-job-0"
	client := NewTClient(addr)
	_, err := client.CreateJob(jobID, int32(request))
	if err != nil {
		fmt.Println("CreateJob Error %v", err)
		return
	}

	startTimes := map[int]time.Time{}
	var costs []time.Duration
	var cpus []float64
	for t := 0; t < request; t++ {
		startTimes[t] = time.Now()

		go func() {
			_, err := client.SendTask(jobID, t)
			if err != nil {
				fmt.Println("SendTask Error %v", err)

			}
		}()

		if t%100 == 0 {
			cpu, _ := cpu.Percent(0, false)
			cpus = append(cpus, cpu[0])
		}
	}
	for t := range client.GetResponse(jobID) {
		costs = append(costs, time.Since(startTimes[t]))
	}
	sort.Slice(costs, func(i, j int) bool { return costs[i] < costs[j] })
	sort.Slice(cpus, func(i, j int) bool { return cpus[i] < cpus[j] })
	fmt.Println("costs: P50: %v, P99: %v", costs[len(costs)/2], costs[len(costs)-1])
	fmt.Println("cpu: P50: %v, P99: %v", cpus[len(cpus)/2], cpus[len(cpus)-1])
	// fmt.Println("")
}
