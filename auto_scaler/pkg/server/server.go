package server

import (
	"context"
	"strconv"
	"time"

	// "flag"

	"github.com/go-logr/logr"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	klogr "k8s.io/klog/v2/klogr"

	pb "poc.telepathy.scale/externalscaler"
	"poc.telepathy.scale/pkg/jobclient"
	"poc.telepathy.scale/pkg/queueclient"
)

const (
	QUEUE_KEY             = "queue_key"
	PARALLEL_KEY          = "parallel_key"
	DEADLINE_KEY          = "deadline_key"
	ESTIMATE_EXE_TIME_KEY = "estimate_exe_time"
	// JOB_KEY      = "job_key"
)

// flag.Set("v", "3")
// //flag.Set("logtostderr", "true")
// flag.Parse()
// klog.InitFlags(nil)

var logger logr.Logger = klogr.New().WithName("mertic-grpc-server")

type server struct {
	pb.ExternalScalerServer
	qc queueclient.IQueueClient
	jc jobclient.IJobClient
}

func (s *server) IsActive(ctx context.Context, obj *pb.ScaledObjectRef) (*pb.IsActiveResponse, error) {
	var qname string
	if qname, ok := obj.ScalerMetadata[QUEUE_KEY]; !ok || qname == "" {
		return nil, status.Error(codes.InvalidArgument, "Medadata: "+QUEUE_KEY+" Must be set and not be empty")
	}
	value := s.qc.IsQueueExist(qname)
	logger.Info("IsActive", value)
	return &pb.IsActiveResponse{
		Result: value,
	}, nil

}

func (s *server) StreamIsActive(obj *pb.ScaledObjectRef, svc pb.ExternalScaler_StreamIsActiveServer) error {
	return nil
}

func (s *server) GetMetricSpec(ctx context.Context, obj *pb.ScaledObjectRef) (*pb.GetMetricSpecResponse, error) {
	return &pb.GetMetricSpecResponse{
		MetricSpecs: []*pb.MetricSpec{&pb.MetricSpec{
			MetricName: "WorkerNum",
			TargetSize: 1,
		}},
	}, nil
}

func (s *server) GetMetrics(ctx context.Context, obj *pb.GetMetricsRequest) (*pb.GetMetricsResponse, error) {
	// Q: Length of Current Queue.
	// T: Average Execution Time for One Task.
	// P: Parallism of Worker Processor.
	// D: Job Deadline.
	// N: Now timestamp.
	// W: Target Worker time.
	// Q * T / (N * P) < D - N
	// W > Q * T / (P * (D - N))
	var qname string
	if qname, ok := obj.ScaledObjectRef.ScalerMetadata[QUEUE_KEY]; !ok || qname == "" {
		return nil, status.Error(codes.InvalidArgument, "Medadata: "+QUEUE_KEY+" Must be set and not be empty")
	}
	var parallel string
	if parallel, ok := obj.ScaledObjectRef.ScalerMetadata[PARALLEL_KEY]; !ok || parallel == "" {
		return nil, status.Error(codes.InvalidArgument, "Medadata: "+PARALLEL_KEY+" Must be set and not be empty")
	}
	var deadline string
	if deadline, ok := obj.ScaledObjectRef.ScalerMetadata[DEADLINE_KEY]; !ok || deadline == "" {
		return nil, status.Error(codes.InvalidArgument, "Medadata: "+DEADLINE_KEY+" Must be set and not be empty")
	}
	var estimatedExeTime string
	if estimatedExeTime, ok := obj.ScaledObjectRef.ScalerMetadata[ESTIMATE_EXE_TIME_KEY]; !ok || estimatedExeTime == "" {
		return nil, status.Error(codes.InvalidArgument, "Medadata: "+estimatedExeTime+" Must be set and not be empty")
	}

	Q := s.qc.GetQueueLength(qname)
	P, err := strconv.Atoi(parallel)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "Medadata: "+PARALLEL_KEY+" can not be decoded as Int")
	}
	T := s.jc.GetAverageExeutionMS(qname)
	// No Task finished yet
	if T <= 0 {
		v, err := strconv.Atoi(estimatedExeTime)
		if err != nil {
			return nil, status.Error(codes.InvalidArgument, "Medadata: "+ESTIMATE_EXE_TIME_KEY+" can not be decoded as Int")
		}
		T = int32(v)
	}
	D, err := time.Parse(time.RFC3339, deadline)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "Medadata: "+DEADLINE_KEY+" cannot be decoded as RFC3339 format")
	}
	N := time.Now()
	remainedMS := D.Sub(N).Nanoseconds() / 1000 / 1000
	W := int64(float64(int64(Q*T)/(int64(P)*remainedMS))*1.1) + 1
	logger.Info("GetMetrics", "Queue length", Q, "Pallelism", P, "Execution time in ms", T,
		"Deadline", deadline, "Now", N.Format(time.RFC3339), "Remaining time", D.Sub(N).Seconds(), "Worker Number", W)
	return &pb.GetMetricsResponse{
		MetricValues: []*pb.MetricValue{
			&pb.MetricValue{
				MetricName:  obj.MetricName,
				MetricValue: W,
			},
		},
	}, nil
}

func NewExternalScalerServer() pb.ExternalScalerServer {
	s := &server{
		qc: queueclient.NewPulsarQueueClient(),
		jc: jobclient.NewPulsarJobClient(),
	}
	return s
}
