package main

import (
	"context"
	"log"
	"time"

	"google.golang.org/grpc"
	pb "telepathy.poc/protos"
)

var addr = "localhost:5001"

func main() {
	conn, err := grpc.Dial(addr)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}
	defer conn.Close()

	request := 100000
	for t := 0; t < request; t++ {
		go func(i int32) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			client := pb.NewFrontendSvcClient(conn)
			client.SendReq(ctx, &pb.ClientRequest{Taskid: i})
		}(int32(t))
	}

}
