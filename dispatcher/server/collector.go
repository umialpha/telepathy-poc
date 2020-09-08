package server

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/nsqio/go-nsq"
	"google.golang.org/protobuf/proto"
	pb "poc.dispatcher/protos"
)

var (
	batchSize      = 100000
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

// func (a *taskResultCollector) Collect(msg *pb.SendResultRequest) error {

// 	bytes, err := proto.Marshal(msg)
// 	if err != nil {
// 		return err
// 	}
// 	err = a.producer.Publish(a.Topic, bytes)
// 	return err
// }

func (a *taskResultCollector) Collect(msg *pb.SendResultRequest) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if msg.TaskState == pb.TaskStateEnum_FINISHED {
		a.rdb.SetNX(ctx, SessionTaskKey(msg.SessionId, msg.TaskId), MSG_STATE_SUCCESS, KEY_EXPIRED_DURATION)
		a.rdb.RPush(ctx, SessionTaskResponse(msg.SessionId, msg.ClientId), msg.SerializedInnerResult)
	} else {
		a.rdb.SetNX(ctx, SessionTaskKey(msg.SessionId, msg.TaskId), MSG_STATE_REQUEUE, KEY_EXPIRED_DURATION)
	}
	return nil
}

func (a *taskResultCollector) batch() {
	for {
		var lst []*nsq.Message
		batchDuration := 1 * time.Second
		timeout := time.After(batchDuration)
		for {
			select {
			case <-timeout:
				goto BREAKLOOP
			case msg := <-a.msgCh:
				lst = append(lst, msg)
				if len(lst) >= batchSize {
					goto BREAKLOOP
				}
			case <-a.stopCh:
				return

			}
		}
	BREAKLOOP:
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
			fmt.Println("writeToRedis Unmarshal error", err)
			msg.Finish()
			continue
		}
		if r.TaskState == pb.TaskStateEnum_FINISHED {
			pipeline.SetNX(ctx, SessionTaskKey(r.SessionId, r.TaskId), "success", KEY_EXPIRED_DURATION)
			pipeline.RPush(ctx, SessionTaskResponse(r.SessionId, r.ClientId), msg.Body)
		} else {
			pipeline.SetNX(ctx, SessionTaskKey(r.SessionId, r.TaskId), "fail", KEY_EXPIRED_DURATION)
		}

	}
	pipeline.Exec(ctx)
	for _, msg := range lst {
		msg.Finish()
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

	// TODO: concurrent initialization
	err := a.initProducer()
	if err != nil {
		return err
	}
	// err = a.initConsumer()
	// if err != nil {
	// 	return err
	// }
	err = a.initRedisClient()
	if err != nil {
		return err
	}

	// go a.batch()

	return nil

}

func (a *taskResultCollector) initConsumer() error {
	nsqConfig := nsq.NewConfig()
	nsqConfig.MaxInFlight = batchSize
	nsqConfig.MsgTimeout = msgTimeout
	nsqConfig.MaxAttempts = uint16(msgMaxAttempts)
	nsqConfig.LookupdPollInterval = 1 * time.Second
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

	opt := &redis.ClusterOptions{
		Addrs:    EnvGetRedisAddrs(),
		Password: EnvGetRedisPass(), // no password set
	}
	a.rdb = redis.NewClusterClient(opt)
	return nil

}

func NewTaskResultCollector(sid string) *taskResultCollector {

	a := &taskResultCollector{
		sessionId:    sid,
		msgCh:        make(chan *nsq.Message, batchSize*2),
		Topic:        sid + "." + "Collector",
		nsqdAddr:     EnvGetNsqdAddr(),
		lookupdAddrs: EnvGetLookupds(),
		stopCh:       make(chan int),
	}
	return a
}
