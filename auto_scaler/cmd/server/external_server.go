package main

import (
	"flag"
	"fmt"
	"net"
	"os"

	"google.golang.org/grpc"

	pb "poc.telepathy.scale/externalscaler"
	"poc.telepathy.scale/pkg/config"
	"poc.telepathy.scale/pkg/server"
)

var (
	port = flag.String("p", "50051", "svc port")
)

func main() {
	flag.Parse()
	fmt.Println("Pulsar addr, admin", os.Getenv(config.PULSAR_ADDR), os.Getenv(config.PULSAR_ADMIN_ADDR))
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
