package main

import (
	"context"

	"google.golang.org/grpc"
	pb "telepathy.poc/protos"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var mpAddr = ""
var mpQueue = ""

type frontendServer struct {
	pb.UnimplementedFrontendSvcServer
}

func (s *frontendServer) SendReq(context.Context, *pb.ClientRequest) (*pb.ClientResponse, error) {
	returni nil, nil
}

func newServer() *frontendServer {
	s := &frontendServer{}

	return s
}

func main() {
	grpcServer := grpc.NewServer()
	pb.RegisterFrontendSvcServer(grpcServer, newServer())
}
