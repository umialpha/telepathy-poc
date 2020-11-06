package main

import (
	"flag"
	"fmt"
	"net"

	"google.golang.org/grpc"

	pb "poc.telepathy.scale/externalscaler"
	"poc.telepathy.scale/pkg/server"
)

var (
	port = flag.String("p", "50051", "svc port")
)

func main() {
	flag.Parse()
	grpcServer := grpc.NewServer()
	pb.RegisterExternalScalerServer(grpcServer, server.NewExternalScalerServer())
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", *port))
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Start Service")
	grpcServer.Serve(lis)
}
