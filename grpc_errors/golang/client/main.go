package main

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
	pb "grpc.errors.poc/protos"
)

func main() {
	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		panic(err)

	}
	client := pb.NewGreeterClient(conn)
	_, err = client.SayHello(context.Background(), &pb.HelloRequest{Name: "lei"})
	if err != nil {
		st, ok := status.FromError(err)
		if !ok {
			fmt.Println("NotStatus", err)
		} else {
			fmt.Println(st.Message())
			fmt.Println(st.Code())
			fmt.Println()
		}
	}
}
