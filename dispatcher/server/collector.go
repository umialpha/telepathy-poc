package server

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/gogo/protobuf/proto"
	"github.com/nsqio/go-nsq"
	pb "poc.dispatcher/protos"
)

var (
	batchSize      = 100
	msgTimeout     = 10 * time.Minute
	msgMaxAttempts = 100
	defaultChannel = "session-Collector"
)

type taskResultCollector struct {
	sessionId    string
	producer     *nsq.Producer
	consumer     *nsq.Consumer
	msgCh        chan *nsq.Message
	rdb          *redis.ClusterClient
	Topic        string
	nsqdAddr     string
	lookupdAddrs []string
	stopCh       chan int
}

func (a *taskResultCollector) Collect(msg *pb.SendResultRequest) error {

	bytes, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	err = a.producer.Publish(a.Topic, bytes)
	return err
}

func (a *taskResultCollector) batch() {
	for {
		batchSize := 100
		var lst []*nsq.Message
		batchDuration := 1 * time.Second
		timeout := time.After(batchDuration)
		for {
			select {
			case <-timeout:
				break
			case msg := <-a.msgCh:
				lst = append(lst, msg)
				if len(lst) >= batchSize {
					break
				}
			case <-a.stopCh:
				return

			}
		}
		if len(lst) != 0 {
			a.writeToRedis(lst)

		}
	}

}

func (a *taskResultCollector) writeToRedis(lst []*nsq.Message) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pipeline := a.rdb.Pipeline()
	for _, msg := range lst {
		r := &pb.SendResultRequest{}
		if err := proto.Unmarshal(msg.Body, r); err != nil {
			fmt.Printf("writeToRedis Unmarshal error", err)
			msg.Finish()
			continue
		}
		if r.TaskState == pb.TaskStateEnum_SUCCESS {
			pipeline.SetNX(ctx, SessionTaskKey(r.SessionId, r.TaskId), "success", KEY_EXPIRED_DURATION)
			pipeline.HSetNX(ctx, SessionTaskResponse(r.SessionId), r.TaskId, msg.Body)
		} else {
			pipeline.SetNX(ctx, SessionTaskKey(r.SessionId, r.TaskId), "fail", KEY_EXPIRED_DURATION)
		}

	}
	_, err := pipeline.Exec(ctx)
	if err != nil {
		for _, msg := range lst {
			msg.Finish()
		}
	}
	return
}

func (a *taskResultCollector) HandleMessage(m *nsq.Message) error {

	if len(m.Body) == 0 {
		// Returning nil will automatically send a FIN command to NSQ to mark the message as processed.
		return nil
	}
	m.DisableAutoResponse()
	// h.timer.Add()
	a.msgCh <- m
	return nil
}

func (a *taskResultCollector) Start() error {

	err := a.initConsumer()
	if err != nil {
		return err
	}

	go a.batch()

	return nil

}

func (a *taskResultCollector) initConsumer() error {
	nsqConfig := nsq.NewConfig()
	nsqConfig.MaxInFlight = batchSize
	nsqConfig.MsgTimeout = msgTimeout
	nsqConfig.MaxAttempts = uint16(msgMaxAttempts)
	c, err := nsq.NewConsumer(a.Topic, defaultChannel, nsqConfig)
	if err != nil {
		return err

	}
	c.AddHandler(a)
	a.consumer = c
	c.ConnectToNSQLookupds(a.lookupdAddrs)
	return nil
}

func (a *taskResultCollector) initProducer() error {
	config := nsq.NewConfig()
	producer, err := nsq.NewProducer(a.nsqdAddr, config)
	if err != nil {
		return err
	}
	a.producer = producer
	return nil
}

func (a *taskResultCollector) initRedisClient() error {
	redisAddr := os.Getenv(REDIS_ADDR_KEY)
	if redisAddr == "" {
		redisAddr = "localhost:6379"

	}
	redisPass := os.Getenv(REDIS_PASSWORD_KEY)
	fmt.Println("redisAddr", redisAddr, redisPass)

	opt := &redis.ClusterOptions{
		Addrs:    []string{redisAddr},
		Password: redisPass, // no password set
	}
	a.rdb = redis.NewClusterClient(opt)
	return nil

}

func NewTaskResultCollector(sid string) *taskResultCollector {

	a := &taskResultCollector{
		sessionId:    sid,
		msgCh:        make(chan *nsq.Message, batchSize*2),
		Topic:        sid + "." + "Collector",
		nsqdAddr:     os.Getenv(ENV_NSQ_NSQD),
		lookupdAddrs: strings.Split(os.Getenv(ENV_NSQ_LOOKUPD), " "),
		stopCh:       make(chan int),
	}
	return a
}
