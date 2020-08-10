package server

import (
	"errors"
	"time"

	"github.com/nsqio/go-nsq"
)

type Fetcher interface {
	Start()
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
	consumers      []*nsq.Consumer
	msgCh          chan Message
	topic          string
	channel        string
	lookupds       []string
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
	//fmt.Println("handle message")
	if len(m.Body) == 0 {
		// Returning nil will automatically send a FIN command to NSQ to mark the message as processed.
		return nil
	}
	m.DisableAutoResponse()
	message := NewNsqMessage(f.topic, f.channel, m)
	f.msgCh <- message
	return nil
}

func (f *nsqFetcher) Start() {
	for _, c := range f.consumers {
		c.ConnectToNSQLookupds(f.lookupds)
	}
}

func NewFetcher(parallel int, bufferCnt int, topic string, channel string, lookupds []string, config *nsq.Config) (Fetcher, error) {

	fetcher := &nsqFetcher{
		msgCh:    make(chan Message, bufferCnt),
		topic:    topic,
		channel:  channel,
		lookupds: lookupds,
	}
	for i := 0; i < parallel; i++ {
		c, err := nsq.NewConsumer(topic, channel, config)
		if err != nil {
			return nil, err

		}
		c.AddHandler(fetcher)
		fetcher.consumers = append(fetcher.consumers, c)
	}
	return fetcher, nil
}
