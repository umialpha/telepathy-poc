package main

import (
	"context"
	"flag"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/nsqio/go-nsq"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	pb "poc.dispatcher/protos"
	"poc.dispatcher/server"
)

var (
	addr      = flag.String("s", "localhost:50051", "server address, `<addr>:<port>`")
	sessionId = flag.String("sid", "ttt-session-5", "working topic")
	nsqdAddr  = flag.String("nsqd", "localhost:4150", "lookupd address")
	duration  = flag.Duration("runfor", 30*time.Second, "duration of time to run, e.g `1h1m10s`, `10ms`")
	conn      = flag.Int("c", 5, "concurrent connections")
	numRPC    = flag.Int("r", 5, "The number of concurrent RPCs on each connection.")
	prepare   = flag.Bool("p", false, "produceMsg mode, default false")
	msgCount  = flag.Int("m", 1000000, "message counts")
	wg        sync.WaitGroup
	cnts      = int32(0)
	fins      = int32(0)
	nomsgs    = int32(0)
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
				resp, err := client.GetWrappedTask(context.Background(), &pb.GetTaskRequest{SessionId: *sessionId})
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
				_, err = client.SendResult(context.Background(), &pb.SendResultRequest{
					SessionId:             *sessionId,
					TaskId:                resp.TaskId,
					TaskState:             pb.TaskStateEnum_FINISHED,
					SerializedInnerResult: []byte{},
				})
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

func produce(topicList []string) {
	config := nsq.NewConfig()
	producer, err := nsq.NewProducer(*nsqdAddr, config)
	if err != nil {
		panic(err.Error())
	}
	// ready and wait to start
	// msg := make([]byte, 100)

	var cnt int
	startTime := time.Now()
	for cnt < *msgCount {
		hello := &pb.EchoRequest{
			Message: fmt.Sprintf("Name-%d", cnt),
		}
		msg, _ := proto.Marshal(hello)
		innerTask := &pb.InnerTask{
			SessionId:   *sessionId,
			ClientId:    "test-client",
			MessageId:   fmt.Sprintf("Msg-%d", cnt),
			ServiceName: "Microsoft.Telepathy.ProtoBuf.Echo",
			MethodName:  "Echo",
			MethodType:  pb.MethodEnum_UNARY,
			Msg:         msg,
		}
		innerBytes, _ := proto.Marshal(innerTask)

		err := producer.Publish(topicList[cnt%len(topicList)], innerBytes)
		if err != nil {
			panic(err.Error())
		}
		cnt++

	}
	fmt.Println("Produce Msg Count %s, qps %s", cnt, float64(cnt)/time.Since(startTime).Seconds())
	forever := make(chan int)
	<-forever
}

func prepareWork() {
	// set redis queue
	rdb := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:    server.EnvGetRedisAddrs(),
		Password: server.EnvGetRedisPass(), // no password set
	})
	key := server.SessionBatchKey(*sessionId)
	batchIds := []string{"client"}
	ctx := context.Background()
	cmd := rdb.SAdd(ctx, key, batchIds)
	_, err := cmd.Result()
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("Prepare Session done")

	// publish to queue
	var topics []string
	for _, bid := range batchIds {
		topics = append(topics, server.GetTopic(*sessionId, bid))
	}
	produce(topics)
}

func consume() {

}

func main() {
	flag.Parse()
	if *prepare {
		prepareWork()
	} else {
		workflow()
	}

}
