package main

import (
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc"
	"google.golang.org/grpc/benchmark/stats"
	"google.golang.org/protobuf/proto"
	"t.poc.v3/mq"
	pb "t.poc.v3/protos"
)

var (
	qAddr       = flag.String("q", "0.0.0.0:9092", "MQ ADDR")
	frontAddr   = flag.String("addr", "localhost:4001", "frontend server ip:port")
	reqNum      = flag.Int("n", 5, "requeset num")
	respTimeout = flag.Int("t", 120, "Get Response Timeout in seconds")
	testOne     = flag.Bool("testOne", false, "test one queue")
	jobQueue    = flag.String("j", "JOB-QUEUE-V3-1", "Job Queue")
)

func endQueueName(queue string) string {
	return queue + "-END"
}

type TClient struct {
	serverAddr string
	client     pb.FrontendSvcClient
	conn       *grpc.ClientConn
	kfclient   *mq.KafkaClient
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

func (c *TClient) CloseJob(jobID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := c.client.CloseJob(ctx, &pb.JobRequest{JobID: jobID})
	if err != nil {
		fmt.Println("Close Job Error:", err)
	}
	return
}

func (c *TClient) SendTask(jobID string, taskID int32) error {
	value := &pb.TaskRequest{
		JobID:  jobID,
		TaskID: int32(taskID),
		Timestamp: &pb.ModifiedTime{
			Client: time.Now().UnixNano(),
		}}
	bytes, err := proto.Marshal(value)
	if err != nil {
		return err
	}
	c.kfclient.Produce(jobID, bytes)
	return err
}

func (c *TClient) GetResponse(jobID string, num int, timeout int) []*pb.TaskResponse {
	writeCh := make(chan []byte, 1000)
	abortCh := make(chan int)
	errorCh := make(chan error)
	timeoutChan := time.After(time.Duration(timeout) * time.Second)
	go c.kfclient.Consume(endQueueName(jobID), endQueueName(jobID), writeCh, errorCh, abortCh)
	var values []*pb.TaskResponse

	stop := false
	for !stop {
		select {
		case <-errorCh:
			stop = true
			break
		case value := <-writeCh:
			resp := &pb.TaskResponse{}
			if err := proto.Unmarshal(value, resp); err != nil {
				fmt.Println("GetResponse Ummarshel error", err)
				stop = true
				break
			}

			//fmt.Println("GerResponse", resp)
			resp.Timestamp.End = time.Now().UnixNano()
			values = append(values, resp)
			if len(values) >= num {
				stop = true
				break
			}
		case <-timeoutChan:
			stop = true
			break
		}
	}
	abortCh <- 1
	return values
}

func (c *TClient) CloseConn() {
	c.conn.Close()
}

func NewTClient(addr string) (*TClient, error) {
	c := &TClient{
		serverAddr: addr,
	}
	conn, err := grpc.Dial(addr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
		return nil, err
	}
	kf, kerr := mq.NewKafkaClient(*qAddr)
	if kerr != nil {
		fmt.Println("Kafka client error", kerr)
		return nil, err
	}
	client := pb.NewFrontendSvcClient(conn)
	c.client = client
	c.conn = conn
	c.kfclient = kf
	return c, nil
}

func NewJobID() (string, error) {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		fmt.Println("NewJobID err", err)
		return "", err
	}
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:]), nil
}

var hopts = stats.HistogramOptions{
	NumBuckets:   2495,
	GrowthFactor: .01,
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

func main() {

	cpuBeg := GetCPUTime()

	flag.Parse()
	fmt.Println("Flags:", *frontAddr, *reqNum)

	client, cerr := NewTClient(*frontAddr)
	if cerr != nil {
		return
	}
	startTime := time.Now()

	var jobID string
	if !*testOne {
		jobID, err := NewJobID()
		fmt.Println("jobID", jobID)
		if err != nil {
			return
		}
		err = client.CreateJob(jobID, int32(*reqNum))
		if err != nil {
			fmt.Println("CreateJob Error", err)
			return
		}
		fmt.Println("Perf CreateJob Duration:", time.Since(startTime))

	} else {
		jobID = *jobQueue
	}

	for i := 0; i < *reqNum; i++ {
		client.SendTask(jobID, int32(i+1))
	}
	go func() {

		var msgCount int
		eventChan := client.kfclient.Producer().Events()
		for e := range eventChan {
			switch e.(type) {
			case *kafka.Message:
				msg := e.(*kafka.Message)
				if msg.TopicPartition.Error != nil {
					fmt.Println("Delever error", msg.TopicPartition.Error)
					return
				}
				msgCount++
				if msgCount >= *reqNum {
					return
				}

			default:
				fmt.Printf("Ignored event")
			}
		}
	}()

	resps := client.GetResponse(jobID, *reqNum, *respTimeout)
	elapsed := time.Since(startTime)
	fmt.Printf("Job Count %v, Duration Sec %v \n", len(resps), elapsed.Seconds())
	fmt.Println("Client CPU utilization Sec:", time.Duration(GetCPUTime()-cpuBeg).Seconds())
	fmt.Println("qps:", float64(len(resps))/float64(elapsed.Seconds()))

	var hists []*stats.Histogram
	for i := 0; i < 5; i++ {
		hists = append(hists, stats.NewHistogram(hopts))
	}
	for _, resp := range resps {
		hists[0].Add(resp.Timestamp.Back - resp.Timestamp.Client)
		hists[1].Add(resp.Timestamp.Worker - resp.Timestamp.Back)
		hists[2].Add(resp.Timestamp.End - resp.Timestamp.Worker)

		hists[3].Add(resp.Timestamp.Worker - resp.Timestamp.Client)
		hists[4].Add(resp.Timestamp.End - resp.Timestamp.Client)
	}

	fmt.Println("Parse Client => Backend Latency")
	parseHist(hists[0])
	fmt.Println("Parse Backend => Worker Latency")
	parseHist(hists[1])
	fmt.Println("Parse Worker => END Latency")
	parseHist(hists[2])

	fmt.Println("Parse Client => Worker Latency")
	parseHist(hists[3])
	fmt.Println("Parse End => End Latency")
	parseHist(hists[4])

}
