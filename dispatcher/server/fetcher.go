package server

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/nsqio/go-nsq"
)

type Fetcher interface {
	Start() error
	Fetch() (Message, error)
	Stop() error
}

var (
	SESSION_QUEUE_CHANNEL = "SESSION_QUEUE_CHANNEL"
	NoMessageError        = errors.New("No Message")
)

func getTopic(sessionId string, batchId string) string {
	return sessionId + "." + batchId
}

type handler struct {
	topic   string
	channel string
	ch      chan Message
	timer   Timer
	msgMgr  *msgMgr
}

func (h *handler) HandleMessage(m *nsq.Message) error {

	if len(m.Body) == 0 {
		// Returning nil will automatically send a FIN command to NSQ to mark the message as processed.
		return nil
	}
	m.DisableAutoResponse()
	message := NewNsqMessage(h.topic, h.channel, m)
	h.msgMgr.Add(message)
	h.ch <- message
	return nil
}

type nsqFetcher struct {
	Fetcher
	sessionId string
	consumers map[string]*nsq.Consumer
	conMtx    sync.Mutex
	msgCh     chan Message
	lookupds  []string
	config    *nsqFetcherConfig
	msgMgr    *msgMgr
	rdb       *redis.ClusterClient
	nsqConfig *nsq.Config
	stopCh    chan int
}

func (f *nsqFetcher) Fetch() (Message, error) {
	var msg Message
	for {
		select {
		case msg = <-f.msgCh:
			break
		case <-time.After(f.config.WaitDurationIfNoMsg):
			msg = nil
			break
		}
		if msg == nil {
			return nil, NoMessageError
		}
		_, ok := f.msgMgr.GetState(msg)
		if !ok {
			return msg, nil
		}
	}

}

func (f *nsqFetcher) Start() error {

	f.nsqConfig = nsq.NewConfig()
	f.nsqConfig.MaxInFlight = f.config.MaxInFlight
	f.nsqConfig.MsgTimeout = f.config.MsgTimeout
	f.nsqConfig.MaxAttempts = f.config.MaxAttempts
	f.refreshTopics()

	go func() {
		ticker := time.NewTicker(1 * time.Second)
		for {
			select {
			case <-f.stopCh:
				ticker.Stop()
			case <-ticker.C:
				f.refreshTopics()
			}
		}
	}()

	return nil
}

func (f *nsqFetcher) Stop() error {
	close(f.stopCh)
	var wg sync.WaitGroup
	for _, consumer := range f.consumers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			consumer.Stop()
		}()

	}
	wg.Wait()
	return nil
}

func (f *nsqFetcher) refreshTopics() {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()
	cmd := f.rdb.SMembers(ctx, SessionBatchKey(f.sessionId))
	batches, err := cmd.Result()
	if err != nil {
		return
	}
	newTopics := make(map[string]string)
	for _, batch := range batches {
		newTopics[getTopic(f.sessionId, batch)] = getTopic(f.sessionId, batch)
	}

	var wg sync.WaitGroup
	// delete out-dated topic
	for topic, consumer := range f.consumers {
		if _, ok := newTopics[topic]; !ok {
			wg.Add(1)
			go func() {
				consumer.Stop()
				wg.Done()
			}()
			delete(f.consumers, topic)
		}
	}
	// add new topic
	for newTopic := range newTopics {
		if _, ok := f.consumers[newTopic]; !ok {
			c, err := nsq.NewConsumer(newTopic, SESSION_QUEUE_CHANNEL, f.nsqConfig)
			if err != nil {
				fmt.Println("Error refreshConsumers", newTopic, err)
				continue
			}
			h := &handler{
				topic:   newTopic,
				channel: SESSION_QUEUE_CHANNEL,
				ch:      f.msgCh,
			}
			c.AddHandler(h)
			f.consumers[newTopic] = c
			c.ConnectToNSQLookupds(f.lookupds)
		}
	}
	wg.Wait()

}

type nsqFetcherConfig struct {
	MaxInFlight         int
	MsgTimeout          time.Duration
	MaxAttempts         uint16
	BufferLen           int
	WaitDurationIfNoMsg time.Duration
}

func NewNsqFetcherConfig() *nsqFetcherConfig {
	c := &nsqFetcherConfig{
		MaxInFlight:         100000,
		MsgTimeout:          10 * time.Minute,
		MaxAttempts:         10,
		BufferLen:           1000,
		WaitDurationIfNoMsg: 1 * time.Second,
	}
	return c
}

func NewNsqFetcher(sessionId string, config *nsqFetcherConfig) (Fetcher, error) {

	fetcher := &nsqFetcher{
		msgCh:     make(chan Message, config.BufferLen),
		sessionId: sessionId,
		config:    config,
		msgMgr:    newMgr(sessionId),
		lookupds:  EnvGetLookupds(),
		stopCh:    make(chan int),
	}

	return fetcher, nil
}
