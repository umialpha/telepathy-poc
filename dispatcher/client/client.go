package main

import (
	"context"
	"flag"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nsqio/go-nsq"
	"google.golang.org/grpc"
	pb "poc.dispatcher/protos"
)

var (
	addr       = flag.String("s", "localhost:50051", "server address, `<addr>:<port>`")
	topic      = flag.String("t", "disp-topic", "working topic")
	channel    = flag.String("ch", "disp-channel", "working channel")
	nsqdAddr   = flag.String("nsqd", "localhost:4150", "lookupd address")
	duration   = flag.Duration("runfor", 30*time.Second, "duration of time to run, e.g `1h1m10s`, `10ms`")
	conn       = flag.Int("c", 10, "concurrent connections")
	numRPC     = flag.Int("r", 10, "The number of concurrent RPCs on each connection.")
	produceMsg = flag.Bool("p", false, "produceMsg mode, default false")

	wg     sync.WaitGroup
	cnts   = int32(0)
	fins   = int32(0)
	nomsgs = int32(0)
)

func runWithConn() {
	conn, err := grpc.Dial(*addr, grpc.WithInsecure())
	if err != nil {
		panic(err)

	}

	endTime := time.Now().Add(*duration)

	for i := 0; i < *numRPC; i++ {
		client := pb.NewDispatcherClient(conn)
		wg.Add(1)
		go func() {
			defer wg.Done()
			cnt := int32(0)
			fin := int32(0)
			nomsg := int32(0)
			for {
			RetryGet:
				if time.Now().After(endTime) {
					atomic.AddInt32(&cnts, cnt)
					atomic.AddInt32(&fins, fin)
					atomic.AddInt32(&nomsgs, nomsg)
					break
				}
				// ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				// defer cancel()
				t := time.Now()
				resp, err := client.GetTask(context.Background(), &pb.TaskRequest{Topic: *topic, Channel: *channel})
				fmt.Println("GetTask cost", time.Since(t))
				if err != nil {
					fmt.Println("GetTask failed, error: ", err)
					nomsg++
					//time.Sleep(time.Second)
					goto RetryGet
				}
				cnt++
				// fmt.Printf("Get task %s, cnt %d\n", resp.MessageID, cnt)
			RetryFinTask:
				// ctx1, cancel1 := context.WithTimeout(context.Background(), 1000*time.Second)
				// defer cancel1()
				// time.Sleep(1 * time.Second)
				t = time.Now()
				_, err = client.FinTask(context.Background(), &pb.FinTaskRequest{
					Topic:   *topic,
					Channel: *channel,
					Result:  pb.TaskResult_FIN, MessageID: resp.MessageID})
				fmt.Println("FinTask cost", time.Since(t))
				if err != nil {
					fmt.Println("FinTask failed, error: ", err)
					//time.Sleep(time.Millisecond)
					goto RetryFinTask
				}
				fin++
				//fmt.Printf("Fin task %s, cnt %d\n", resp.MessageID, fin)
			}

		}()
	}
}

func workflow() {

	start := time.Now()
	for i := 0; i < *conn; i++ {
		runWithConn()
	}
	wg.Wait()

	milli := time.Now().Sub(start).Seconds()
	fmt.Printf("Workflow Duration: %.2f sec, Get Task %d, Finish Task %d, NoMsg %v, QPS: %.2f \n", milli, cnts, fins, nomsgs, float32(fins)/float32(milli))
	forever := make(chan int)
	<-forever
}

func produceWork() {
	config := nsq.NewConfig()
	producer, err := nsq.NewProducer(*nsqdAddr, config)
	if err != nil {
		panic(err.Error())
	}
	// ready and wait to start
	msg := make([]byte, 100)
	var msgCount int64
	endTime := time.Now().Add(*duration)
	for {
		err := producer.Publish(*topic, msg)
		if err != nil {
			panic(err.Error())
		}
		msgCount++
		if time.Now().After(endTime) {
			break
		}
	}
	fmt.Println("Produce Msg Count", msgCount)
	forever := make(chan int)
	<-forever
}

func main() {
	flag.Parse()
	if *produceMsg {
		produceWork()
	} else {
		workflow()
	}

}
