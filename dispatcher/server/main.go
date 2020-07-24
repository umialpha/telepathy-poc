package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/nsqio/go-nsq"
	"google.golang.org/grpc"
	pb "poc.dispatcher/protos"
)

var (
	lookupds = flag.String("lookupd", "localhost:4161", "lookupd address, `<addr>:<port> <addr>:<port>`")
	port     = flag.String("p", "50051", "svc port")
)

type consumer struct {
	topic       string
	channel     string
	lookupds    []string
	bufferCnt   int
	buffer      chan *nsq.Message
	c           *nsq.Consumer
	msgDelegate nsq.MessageDelegate
}

func (c *consumer) Fetch() (*nsq.Message, error) {
	select {
	case m := <-c.buffer:
		return m, nil
	case <-time.After(1 * time.Second):
		return nil, errors.New("No message at the moment")
	}

}

func (c *consumer) Fin(id nsq.MessageID) error {
	msg := nsq.NewMessage(id, []byte{})
	msg.Delegate = c.msgDelegate
	msg.Finish()
	return nil
}

func (c *consumer) Fail(id nsq.MessageID) error {
	msg := nsq.NewMessage(id, []byte{})
	msg.Delegate = c.msgDelegate
	msg.Requeue(-1)
	return nil
}

func (h *consumer) HandleMessage(m *nsq.Message) error {
	fmt.Println("consumer handle message")
	if len(m.Body) == 0 {
		// Returning nil will automatically send a FIN command to NSQ to mark the message as processed.
		return nil
	}
	m.DisableAutoResponse()
	if h.msgDelegate == nil {
		h.msgDelegate = m.Delegate
	}
	h.buffer <- m
	// Returning a non-nil error will automatically send a REQ command to NSQ to re-queue the message.
	return nil
}

func NewConsumer(topic string, channel string, lookupds []string, bufferCnt int) (*consumer, error) {
	c := &consumer{
		topic:     topic,
		channel:   channel,
		lookupds:  lookupds,
		bufferCnt: bufferCnt,
		buffer:    make(chan *nsq.Message, bufferCnt),
	}

	nsqc, err := nsq.NewConsumer(topic, channel, nsq.NewConfig())
	if err != nil {
		return nil, err
	}
	c.c = nsqc
	nsqc.AddHandler(c)
	nsqc.ConnectToNSQLookupds(lookupds)
	return c, nil
}

// Consumer Cache

func getKey(topic string, channel string) string {
	return topic + "." + channel
}

type ConsumerCache map[string]*consumer

func (cc ConsumerCache) GetConsumer(topic string, channel string) (*consumer, error) {

	if _, ok := cc[getKey(topic, channel)]; !ok {
		c, err := NewConsumer(topic, channel, strings.Split(*lookupds, " "), 10)
		if err != nil {
			return nil, err
		}
		cc[getKey(topic, channel)] = c
	}
	return cc[getKey(topic, channel)], nil
}

type server struct {
	pb.DispatcherServer
	consumerCache ConsumerCache
	cacheMu       sync.RWMutex
}

func (s *server) GetTask(ctx context.Context, in *pb.TaskRequest) (*pb.TaskResponse, error) {
	s.cacheMu.Lock()
	c, err := s.consumerCache.GetConsumer(in.Topic, in.Channel)
	s.cacheMu.Unlock()
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	msg, err := c.Fetch()
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	msgID := [nsq.MsgIDLength]byte(msg.ID)
	resp := &pb.TaskResponse{Payload: msg.Body, MessageID: msgID[:]}
	return resp, nil
}

func (s *server) FinTask(ctx context.Context, in *pb.FinTaskRequest) (*pb.FinTaskResponse, error) {
	s.cacheMu.Lock()
	c, err := s.consumerCache.GetConsumer(in.Topic, in.Channel)
	s.cacheMu.Unlock()
	if err != nil {
		fmt.Println(err)
		return nil, err
	}

	var msgID [nsq.MsgIDLength]byte
	copy(msgID[:], in.MessageID[:nsq.MsgIDLength])
	msgID = nsq.MessageID(msgID)

	switch in.Result {
	case pb.TaskResult_FIN:
		c.Fin(msgID)
		break
	case pb.TaskResult_FAIL:
		c.Fail(msgID)
		break
	default:
		return &pb.FinTaskResponse{}, errors.New("Invalid TaskResult")
	}
	return &pb.FinTaskResponse{}, nil

}

func newServer() pb.DispatcherServer {
	s := &server{}
	s.consumerCache = ConsumerCache{}
	// consumer, err := NewConsumer("disp-topic", "disp-channel", strings.Split(*lookupds, " "), 10)
	// if err != nil {
	// 	panic(err)
	// }
	// s.consumerCache[getKey("disp-topic", "disp-channel")] = consumer
	return s

}

func main() {
	flag.Parse()
	grpcServer := grpc.NewServer()
	pb.RegisterDispatcherServer(grpcServer, newServer())
	lis, err := net.Listen("tcp", fmt.Sprintf(":%s", *port))
	if err != nil {
		fmt.Println(err)
		return
	}
	grpcServer.Serve(lis)
}
