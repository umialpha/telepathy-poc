package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"time"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/benchmark/stats"
	pb "telepathy.poc/protos"
)

var (
	frontAddr = flag.String("addr", "localhost:4001", "frontend server ip:port")
	reqNum    = flag.Int("n", 1, "requeset num")
	//numRPC    = flag.Int("r", 1, "The number of concurrent RPCs on each connection.")
	numConn = flag.Int("c", 5, "The number of parallel connections.")
	//warmupDur   = flag.Int("w", 10, "Warm-up duration in seconds")
	respTimeout = flag.Int("t", 120, "Get Response Timeout in seconds")
)

type TClient struct {
	serverAddr string
	client     pb.FrontendSvcClient
	conn       *grpc.ClientConn
}

func (c *TClient) CreateJob(jobID string, reqNum int32) error {
	req := &pb.JobRequest{
		JobID:     jobID,
		ReqNum:    reqNum,
		Timestamp: &pb.ModifiedTime{Client: time.Now().UnixNano()},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := c.client.CreateJob(ctx, req)
	if err != nil {
		fmt.Println("Create Job Err", err)
		return err
	}
	return nil
}

func (c *TClient) SendTask(jobID string, taskID int) (*pb.TaskResponse, error) {
	req := &pb.TaskRequest{
		JobID:     jobID,
		TaskID:    int32(taskID),
		Timestamp: &pb.ModifiedTime{Client: time.Now().UnixNano()},
	}

	//ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	//defer cancel()
	resp, err := c.client.SendTask(context.Background(), req)
	if err != nil {
		fmt.Println("Send Task Err", err)
		return nil, err
	}
	return resp, nil
}

func (c *TClient) GetResponse(jobID string, reqNum int32) chan *pb.TaskResponse {
	ch := make(chan *pb.TaskResponse, 1000)

	//ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	stream, err := c.client.GetResponse(context.Background(), &pb.JobRequest{JobID: jobID, ReqNum: reqNum})
	if err != nil {
		log.Fatalf("GetResponse error %v.\n", err)
		close(ch)
		//cancel()
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
			ch <- resp

		}
		//cancel()
	}()
	return ch
}

func (c *TClient) CloseJob(jobID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := c.client.CloseJob(ctx, &pb.JobRequest{JobID: jobID})
	if err != nil {
		fmt.Println("Close Job Error:", err)
	}
	return
}

func (c *TClient) CloseConn() {
	c.conn.Close()
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
	c.conn = conn
	return c
}

func NewJobID() (string, error) {
	b := make([]byte, 8)
	_, err := rand.Read(b)
	if err != nil {
		fmt.Println("NewJobID err", err)
		return "", err
	}
	return fmt.Sprintf("%x-%x-%x", b[0:4], b[4:6], b[6:8]), nil
}

func GetCPUTime() int64 {
	var ts unix.Timespec
	if err := unix.ClockGettime(unix.CLOCK_PROCESS_CPUTIME_ID, &ts); err != nil {
		fmt.Println(err)
		return 0
	}
	return ts.Nano()
}

func parseHist(hist *stats.Histogram) {
	fmt.Printf("Latency: (50/90/99 %%ile): %v/%v/%v\n",
		time.Duration(median(.5, hist)),
		time.Duration(median(.9, hist)),
		time.Duration(median(.99, hist)))
}

func median(percentile float64, h *stats.Histogram) int64 {
	need := int64(float64(h.Count) * percentile)
	have := int64(0)
	for _, bucket := range h.Buckets {
		count := bucket.Count
		if have+count >= need {
			percent := float64(need-have) / float64(count)
			return int64((1.0-percent)*bucket.LowBound + percent*bucket.LowBound*(1.0+hopts.GrowthFactor))
		}
		have += bucket.Count
	}
	panic("should have found a bound")
}

var hopts = stats.HistogramOptions{
	NumBuckets:   2495,
	GrowthFactor: .01,
}

func main() {
	//
	cpuBeg := GetCPUTime()
	flag.Parse()
	fmt.Println("Flags:", *frontAddr, *reqNum /*numRPC,*/, *numConn)
	jobID, err := NewJobID()
	fmt.Println("jobID", jobID)
	if err != nil {
		return
	}
	var clients []*TClient
	for i := 0; i < *numConn; i++ {
		clients = append(clients, NewTClient(*frontAddr))
	}
	defer func() {
		for _, c := range clients {
			c.CloseConn()
		}
	}()
	clients[0].CreateJob(jobID, int32(*reqNum))
	defer clients[0].CloseJob(jobID)

	//startTime := time.Now()

	for t := 0; t < *reqNum; t++ {
		go clients[t%len(clients)].SendTask(jobID, t)
	}

	respChan := clients[0].GetResponse(jobID, int32(*reqNum))
	var resps []*pb.TaskResponse
	stop := false
	minStart := float64(time.Now().UnixNano())
	maxEnd := float64(-1)
	for stop == false {
		select {
		case resp, ok := <-respChan:
			if !ok {
				stop = true
				break
			}
			resp.Timestamp.End = time.Now().UnixNano()
			minStart = math.Min(float64(resp.Timestamp.Client), minStart)
			maxEnd = math.Max(float64(resp.Timestamp.Worker), maxEnd)

			resps = append(resps, resp)
		case <-time.After(time.Second * time.Duration((*respTimeout))):
			fmt.Printf("Get Response Timeout, expected: %v, actual: %v\n", int32(*reqNum), len(resps))
			stop = true
			break

		}
	}
	//elapsed := time.Since(startTime)
	elapsed := time.Duration(maxEnd - minStart)
	fmt.Printf("Job Count %v, Duration Sec %v \n", len(resps), elapsed.Seconds())
	fmt.Println("Client CPU utilization Sec:", time.Duration(GetCPUTime()-cpuBeg).Seconds())
	fmt.Println("qps:", float64(len(resps))/float64(elapsed.Seconds()))

	var hists []*stats.Histogram
	for i := 0; i < 5; i++ {
		hists = append(hists, stats.NewHistogram(hopts))
	}
	for _, resp := range resps {
		hists[0].Add(resp.Timestamp.Front - resp.Timestamp.Client)
		//hists[1].Add(resp.Timestamp.Back - resp.Timestamp.Front)
		//hists[2].Add(resp.Timestamp.Worker - resp.Timestamp.Back)

		hists[2].Add(resp.Timestamp.Worker - resp.Timestamp.Front)
		hists[3].Add(resp.Timestamp.End - resp.Timestamp.Worker)
		hists[4].Add(resp.Timestamp.End - resp.Timestamp.Client)
	}

	fmt.Println("Parse Client => Frontend Latency")
	parseHist(hists[0])

	// fmt.Println("Parse Frontend => Backend Latency")
	// parseHist(hists[1])

	fmt.Println("Parse Backend => Worker Latency")
	parseHist(hists[2])

	fmt.Println("Parse Worker => Client Latency")
	parseHist(hists[3])
	fmt.Println("Parse End => End Latency")
	parseHist(hists[4])
}
