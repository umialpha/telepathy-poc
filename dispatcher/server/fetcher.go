package server

import (
	"errors"
	"fmt"
	"time"

	"github.com/nsqio/go-nsq"
)

type Fetcher interface {
	Fetch() (Message, error)
}

var (
	NoMessageError = errors.New("No Message")
)

func getMessageID(topic string, channel string, msgID nsq.MessageID) string {
	return topic + "." + channel + "." + string(msgID[:])
}

type nsqFetcher struct {
	Fetcher
	consumer       *nsq.Consumer
	msgCh          chan Message
	topic          string
	channel        string
	consumerConfig *nsq.Config
}

func (f *nsqFetcher) Fetch() (Message, error) {
	select {
	case v := <-f.msgCh:
		return v, nil
	case <-time.After(time.Millisecond):
		return nil, NoMessageError
	}
}

func (f *nsqFetcher) HandleMessage(m *nsq.Message) error {
	fmt.Println("handle message")
	if len(m.Body) == 0 {
		// Returning nil will automatically send a FIN command to NSQ to mark the message as processed.
		return nil
	}
	m.DisableAutoResponse()
	message := NewNsqMessage(f.topic, f.channel, m)
	f.msgCh <- message
	return nil
}

func NewFetcher(topic string, channel string, lookupds []string, config *nsq.Config) (Fetcher, error) {

	c, err := nsq.NewConsumer(topic, channel, nsq.NewConfig())
	if err != nil {
		return nil, err
	}
	fetcher := &nsqFetcher{
		msgCh:   make(chan Message, 100000),
		topic:   topic,
		channel: channel,
	}
	c.AddHandler(fetcher)
	c.ConnectToNSQLookupds(lookupds)
	fetcher.consumer = c

	return fetcher, nil
}
