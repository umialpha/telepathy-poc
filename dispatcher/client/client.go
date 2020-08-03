package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"github.com/nsqio/go-nsq"
	"google.golang.org/grpc"
	pb "poc.dispatcher/protos"
)

var (
	addr     = flag.String("s", "localhost:50051", "server address, `<addr>:<port>`")
	topic    = flag.String("t", "disp-topic", "working topic")
	channel  = flag.String("ch", "disp-channel", "working channel")
	nsqdAddr = flag.String("nsqd", "localhost:4150", "lookupd address")
	duration = flag.Duration("runfor", 30*time.Second, "duration of time to run, e.g `1h1m10s`, `10ms`")
)

func workflow() {
	conn, err := grpc.Dial(*addr, grpc.WithInsecure())
	if err != nil {
		panic(err)

	}
	client := pb.NewDispatcherClient(conn)
	cnt := 0
	fin := 0
	endTime := time.Now().Add(*duration)
	start := time.Now()
	for {
		if time.Now().After(endTime) {
			break
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
	RetryGet:
		resp, err := client.GetTask(ctx, &pb.TaskRequest{Topic: *topic, Channel: *channel})
		if err != nil {
			fmt.Println("GetTask failed, error: ", err)

			//time.Sleep(time.Second)
			goto RetryGet
		}
		cnt++
		fmt.Printf("Get task %s, cnt %d\n", resp.MessageID, cnt)

		ctx1, cancel1 := context.WithTimeout(context.Background(), 1000*time.Second)
		defer cancel1()
		// time.Sleep(1 * time.Second)
	RetryFinTask:
		_, err = client.FinTask(ctx1, &pb.FinTaskRequest{
			Topic:   *topic,
			Channel: *channel,
			Result:  pb.TaskResult_FIN, MessageID: resp.MessageID})
		if err != nil {
			fmt.Println("FinTask failed, error: ", err)
			//time.Sleep(time.Millisecond)
			goto RetryFinTask
		}
		fin++
		fmt.Printf("Fin task %s, cnt %d\n", resp.MessageID, fin)
	}
	milli := time.Now().Sub(start).Seconds()
	fmt.Printf("Workflow Duration: %.2f sec, Get Task %d, Finish Task %d, QPS: %.2f \n", milli, cnt, fin, float32(fin)/float32(milli))
}

func produceWork() {
	config := nsq.NewConfig()
	producer, err := nsq.NewProducer(*nsqdAddr, config)
	if err != nil {
		panic(err.Error())
	}
	// ready and wait to start
	msg := make([]byte, 100)
	var msgCount int64
	endTime := time.Now().Add(*duration)
	for {
		err := producer.Publish(*topic, msg)
		if err != nil {
			panic(err.Error())
		}
		msgCount++
		if time.Now().After(endTime) {
			break
		}
	}
	fmt.Println("Produce Msg Count", msgCount)
}

func main() {
	workflow()
}
