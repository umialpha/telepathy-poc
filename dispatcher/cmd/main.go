package main

import (
	"flag"
	"fmt"
	"net"
	"strings"

	"google.golang.org/grpc"

	pb "poc.dispatcher/protos"
	"poc.dispatcher/server"
)

var (
	lookupds = flag.String("lookupd", "localhost:4161", "lookupd address, `<addr>:<port> <addr>:<port>`")
	port     = flag.String("p", "50051", "svc port")
)

func main() {
	flag.Parse()
	grpcServer := grpc.NewServer()
	pb.RegisterDispatcherServer(grpcServer, server.NewServer(strings.Split(*lookupds, " ")))
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", *port))
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Start Service")
	grpcServer.Serve(lis)
}
