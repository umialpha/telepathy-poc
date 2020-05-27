package main

import (
	"context"

	"google.golang.org/grpc"
	pb "telepathy.poc/protos"
)

func endQueaueName(que string) string {
	return que + "-END"
}

type WorkerServer struct {
	pb.UnimplementedWorkerSvcServer
	kfclient interface{}
}

func (w *WorkerServer) SendTask(ctx context.Context, req *pb.TaskRequest) (*pb.TaskResponse, error) {
	go w.doWork(req)
	return &pb.TaskResponse{JobID: req.JobID, TaskID: req.TaskID}, nil
}

func (w *WorkerServer) doWork( req *pb.TaskRequest) {
	kfclient.Produce(endQueaueName(req.JobID), req.TaskID)
}

func newServer() *WorkerServer {
	return &WorkerServer{}
}

func main() {
	grpcServer := grpc.NewServer()
	pb.RegisterWorkerSvcServer(grpcServer, newServer())
}
