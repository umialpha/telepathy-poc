package main

import (
	"context"
	"flag"
	"log"
	"time"

	"google.golang.org/grpc"

	pb "poc.telepathy.scale/externalscaler"
)

var (
	addr = flag.String("s", "localhost:50051", "server address, `<addr>:<port>`")
)

func main() {
	conn, err := grpc.Dial(*addr, grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)

	}
	client := pb.NewExternalScalerClient(conn)
	queueName := "test-job"
	resp, err := client.GetMetrics(context.Background(), &pb.GetMetricsRequest{
		MetricName: "DeadlineMetric",
		ScaledObjectRef: &pb.ScaledObjectRef{
			Name:      "test-job",
			Namespace: "default",
			ScalerMetadata: map[string]string{
				"queue_key":         queueName,
				"parallel_key":      "1",
				"deadline_key":      time.Now().Add(time.Minute * 10).Format(time.RFC3339),
				"job_key":           queueName,
				"estimate_exe_time": "1000",
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Println(resp.MetricValues[0].MetricValue)

}

// QUEUE_KEY    = "queue_key"
// 	PARALLEL_KEY = "parallel_key"
// 	DEADLINE_KEY = "deadline_key"
// 	JOB_KEY      = "job_key"
