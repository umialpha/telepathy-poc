package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"google.golang.org/grpc"

	pb "poc.telepathy.scale/externalscaler"
	"poc.telepathy.scale/pkg/jobclient"
)

var (
	addr       = flag.String("s", "localhost:50051", "server address, `<addr>:<port>`")
	pulsarAddr = flag.String("pulsar", "pulsar://localhost:6650", "pulsar hostname:port")
)

func publishMsg(jobid string, msgNum int) {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: *pulsarAddr,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: jobid,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	for i := 0; i < msgNum; i++ {
		producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte(fmt.Sprintf("hello")),
		})
	}

}

func finishMsg(jobid string, msgNum int, costMs int32) {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: *pulsarAddr,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()
	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: jobclient.MetricQueueName(jobid),
	})
	if err != nil {
		log.Fatal(err)
	}
	defer producer.Close()
	for i := 0; i < msgNum; i++ {
		producer.Send(context.Background(), &pulsar.ProducerMessage{
			Payload: []byte(fmt.Sprintf("%d", costMs)),
		})
	}
}

func main() {
	flag.Parse()
	conn, err := grpc.Dial(*addr, grpc.WithInsecure())
	if err != nil {
		log.Fatal(err)

	}
	defer conn.Close()
	client := pb.NewExternalScalerClient(conn)
	queueName := fmt.Sprintf("test-job-%v", time.Now().Nanosecond()/1000)
	fmt.Println("jobid", queueName)
	// resp, err := client.GetMetrics(context.Background(), &pb.GetMetricsRequest{
	// 	MetricName: "DeadlineMetric",
	// 	ScaledObjectRef: &pb.ScaledObjectRef{
	// 		Name:      "test-job",
	// 		Namespace: "default",
	// 		ScalerMetadata: map[string]string{
	// 			"queue_key":         queueName,
	// 			"parallel_key":      "1",
	// 			"deadline_key":      time.Now().Add(time.Second * 1000).Format(time.RFC3339),
	// 			"job_key":           queueName,
	// 			"estimate_exe_time": "1000",
	// 		},
	// 	},
	// })
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// log.Println("Worker Number", resp.MetricValues[0].MetricValue)

	// Publish 100 Message
	publishMsg(queueName, 100)
	// Average Time 1000ms
	finishMsg(queueName, 50, 1000)

	time.Sleep(time.Second)
	resp, err := client.GetMetrics(context.Background(), &pb.GetMetricsRequest{
		MetricName: "DeadlineMetric",
		ScaledObjectRef: &pb.ScaledObjectRef{
			Name:      "test-job",
			Namespace: "default",
			ScalerMetadata: map[string]string{
				"queue_key":         queueName,
				"parallel_key":      "1",
				"deadline_key":      time.Now().Add(time.Second * 1).Format(time.RFC3339),
				"job_key":           queueName,
				"estimate_exe_time": "1000",
			},
		},
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Worker Number", resp.MetricValues[0].MetricValue)

}

// QUEUE_KEY    = "queue_key"
// 	PARALLEL_KEY = "parallel_key"
// 	DEADLINE_KEY = "deadline_key"
// 	JOB_KEY      = "job_key"
