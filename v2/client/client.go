package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"time"

	"github.com/shirou/gopsutil/cpu"
	"google.golang.org/grpc"
	pb "telepathy.poc/protos"
)

type TClient struct {
	serverAddr string
	client     pb.FrontendSvcClient
}

func (c *TClient) CreateJob(jobID string, reqNum int32) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c.client.CreateJob(ctx, &pb.JobRequest{JobID: jobID, ReqNum: reqNum})
}

func (c *TClient) SendTask(jobID string, taskID int) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	c.client.SendTask(ctx, &pb.TaskRequest{JobID: jobID, TaskID: int32(taskID)})
}

func (c *TClient) GetResponse(jobID string) chan int {
	ch := make(chan int, 10)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	stream, err := c.client.GetResponse(ctx, &pb.JobRequest{JobID: jobID})
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
	addr := os.Getenv("FRONT_ADDR")
	request, _ := strconv.Atoi(os.Getenv("REQ_NUM"))
	jobID := os.Getenv("JOB_ID")
	client := NewTClient(addr)
	client.CreateJob(jobID, int32(request))
	startTimes := map[int]time.Time{}
	var costs []time.Duration
	var cpus []float64
	for t := 0; t < request; t++ {
		startTimes[t] = time.Now()
		go client.SendTask(jobID, t)
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
