package main

import (
	"context"

	"google.golang.org/grpc"
	pb "telepathy.poc/protos"
)

type WorkerServer struct {
	pb.UnimplementedWorkerSvcServer
}

func SendTask(ctx context.Context, req *pb.BackendTaskRequest) (*pb.BackendTaskResponse, error) {
	return &pb.BackendTaskResponse{TaskID: req.TaskID}, nil
}

func newServer() *WorkerServer {
	return &WorkerServer{}
}

func main() {
	grpcServer := grpc.NewServer()
	pb.RegisterWorkerSvcServer(grpcServer, newServer())
}
