package main

import (
	"context"
	"flag"
	"fmt"
	"time"

	"google.golang.org/grpc"
	pb "poc.dispatcher/protos"
)

var (
	addr    = flag.String("server addr", "localhost:50051", "server address, `<addr>:<port>`")
	topic   = flag.String("t", "disp-topic", "working topic")
	channel = flag.String("ch", "disp-channel", "working channel")
)

func main() {
	conn, err := grpc.Dial(*addr, grpc.WithInsecure())
	if err != nil {
		panic(err)

	}
	client := pb.NewDispatcherClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := client.GetTask(ctx, &pb.TaskRequest{Topic: *topic, Channel: *channel})
	if err != nil {
		panic(err)
	}
	fmt.Printf("task %s, payload size: %d\n", resp.MessageID, len(resp.Payload))

	ctx1, cancel1 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel1()
	_, err = client.FinTask(ctx1, &pb.FinTaskRequest{
		Topic:   *topic,
		Channel: *channel,
		Result:  pb.TaskResult_FIN, MessageID: resp.MessageID})
	if err != nil {
		panic(err)
	}

}
