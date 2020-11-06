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

func (s *server) createFetcherIfNotExist(sid string) (Fetcher, error) {
	var fetcher Fetcher
	var ok bool
	var err error
	s.fMtx.Lock()
	defer s.fMtx.Unlock()
	if _, ok = s.fetchers[sid]; !ok {
		fetcherConfig := NewNsqFetcherConfig()
		fetcher, err = NewNsqFetcher(sid, fetcherConfig)
		if err != nil {
			fmt.Println("NewNsqFetcher error", err)
			return nil, err
		}

		s.fetchers[sid] = fetcher
		err = fetcher.Start()
		if err != nil {
			return nil, err
		}
	}
	return s.fetchers[sid], nil
}

func (s *server) createCollectorIfNotExists(sid string) (*taskResultCollector, error) {
	var err error
	s.cMtx.Lock()
	defer s.cMtx.Unlock()
	if _, ok := s.collectors[sid]; !ok {
		collector := NewTaskResultCollector(sid)
		s.collectors[sid] = collector
		err = collector.Start()
		if err != nil {
			fmt.Println("Error Start collector", err)
			return nil, err
		}
	}
	return s.collectors[sid], nil
}

func (s *server) GetWrappedTask(ctx context.Context, in *pb.GetTaskRequest) (*pb.WrappedTask, error) {

	// create fetcher if not exists
	fetcher, err := s.createFetcherIfNotExist(in.SessionId)
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
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
	collector, err := s.createCollectorIfNotExists(sid)
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	err = collector.Collect(in)
	if err != nil {
		return nil, status.Errorf(codes.Internal, err.Error())
	}
	return &empty.Empty{}, nil

}

func NewServer() pb.DispatcherServer {

	s := &server{
		fetchers:   make(map[string]Fetcher),
		collectors: make(map[string]*taskResultCollector),
	}
	return s

}
