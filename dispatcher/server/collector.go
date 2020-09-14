package server

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
	pb "poc.dispatcher/protos"
)

var (
	batchSize      = 1000
	msgTimeout     = 10 * time.Minute
	msgMaxAttempts = 100
	defaultChannel = "session-Collector"
)

type taskResultCollector struct {
	sessionId string
	rdb       *redis.ClusterClient
	stopCh    chan int
	respCh    chan *pb.SendResultRequest
}

func (a *taskResultCollector) writeToRedis(lst []*pb.SendResultRequest) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	pipeline := a.rdb.Pipeline()
	for _, msg := range lst {
		if msg.TaskState == pb.TaskStateEnum_FINISHED {
			pipeline.SetNX(ctx, SessionTaskKey(msg.SessionId, msg.TaskId), MSG_STATE_SUCCESS, KEY_EXPIRED_DURATION)
			pipeline.RPush(ctx, SessionTaskResponse(msg.SessionId, msg.ClientId), msg.SerializedInnerResult)
		} else {
			pipeline.SetNX(ctx, SessionTaskKey(msg.SessionId, msg.TaskId), MSG_STATE_REQUEUE, KEY_EXPIRED_DURATION)
		}
		pipeline.Exec(ctx)

	}
	pipeline.Exec(ctx)
	return
}

func (a *taskResultCollector) Collect(msg *pb.SendResultRequest) error {
	a.writeToRedis([]*pb.SendResultRequest{msg})
	return nil
}

func (a *taskResultCollector) CollectAsync(msg *pb.SendResultRequest) error {
	a.respCh <- msg
	return nil
}

func (a *taskResultCollector) batch() {
	for {
		var lst []*pb.SendResultRequest
		batchDuration := 1 * time.Second
		timeout := time.After(batchDuration)
		for {
			select {
			case <-timeout:
				goto BREAKLOOP
			case msg := <-a.respCh:
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

func (a *taskResultCollector) Start() error {

	err := a.initRedisClient()
	if err != nil {
		return err
	}

	go a.batch()

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
		sessionId: sid,
		respCh:    make(chan *pb.SendResultRequest),
		stopCh:    make(chan int),
	}
	return a
}
