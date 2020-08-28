package server

import (
	"context"
	"fmt"
	"sync"

	"github.com/golang/protobuf/ptypes/empty"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	pb "poc.dispatcher/protos"
)

var (

// Default Redis key expire time

)

type server struct {
	pb.DispatcherServer
	fetchers   map[string]Fetcher
	fMtx       sync.RWMutex
	collectors map[string]*taskResultCollector
	cMtx       sync.RWMutex
}

func (s *server) GetWrappedTask(ctx context.Context, in *pb.GetTaskRequest) (*pb.WrappedTask, error) {

	// create fetcher if not exists
	sid := in.SessionId
	var fetcher Fetcher
	var err error
	s.fMtx.Lock()
	if _, ok := s.fetchers[sid]; !ok {
		fetcherConfig := NewNsqFetcherConfig()
		fetcher, err = NewNsqFetcher(sid, fetcherConfig)
		if err != nil {
			fmt.Println("NewNsqFetcher error", err)
			s.fMtx.Unlock()
			return nil, status.Errorf(codes.Internal, err.Error())
		}

		s.fetchers[sid] = fetcher
		err = fetcher.Start()
		if err != nil {
			s.fMtx.Unlock()
			return nil, status.Errorf(codes.Internal, err.Error())
		}
	}
	s.fMtx.Unlock()

	// fetch the msg
	msg, err := fetcher.(Fetcher).Fetch()
	if err != nil {
		fmt.Println("Fetch msg Failed, reason: ", err)
		if err == NoMessageError {
			return &pb.WrappedTask{SessionState: pb.SessionStateEnum_TEMP_NO_TASK}, nil
		} else {
			return nil, status.Errorf(codes.Internal, err.Error())
		}
	}

	resp := &pb.WrappedTask{
		SessionId:           in.SessionId,
		TaskId:              msg.GetID().String(),
		SerializedInnerTask: msg.GetPayload(),
		SessionState:        pb.SessionStateEnum_RUNNING,
	}

	return resp, nil
}
func (s *server) SendResult(ctx context.Context, in *pb.SendResultRequest) (*empty.Empty, error) {
	sid := in.SessionId
	var collector *taskResultCollector
	var err error
	s.cMtx.Lock()
	if collector, ok := s.collectors[sid]; !ok {
		collector = NewTaskResultCollector(in.SessionId)
		s.collectors[sid] = collector
		err = collector.Start()
		if err != nil {
			s.cMtx.Unlock()
			fmt.Println("Error Start collector", err)
			return nil, status.Errorf(codes.Internal, err.Error())
		}
	}
	s.fMtx.Unlock()
	err = collector.Collect(in)
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return &empty.Empty{}, nil

}

func NewServer(nsqlookups []string) pb.DispatcherServer {

	s := &server{
		fetchers:   make(map[string]Fetcher),
		collectors: make(map[string]*taskResultCollector),
	}
	return s

}
