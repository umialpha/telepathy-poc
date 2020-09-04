package server

import (
	"strings"
	"time"

	"github.com/nsqio/go-nsq"
)

type nsqMsg struct {
	Message
	nsq     *nsq.Message
	topic   string
	channel string
}

func (m *nsqMsg) GetID() MessageID {
	id := strings.Join([]string{m.topic, m.channel, string(m.nsq.ID[:])}, ".")
	return MessageID(id)
}

func (m *nsqMsg) GetPayload() []byte {
	return m.nsq.Body
}

func (m *nsqMsg) IsExpired() bool {
	return true
}

func (m *nsqMsg) ExpiredTime() time.Time {
	return time.Unix(m.nsq.Timestamp, 0)
}

func (m *nsqMsg) Touch() {
	m.nsq.Touch()
}

func (m *nsqMsg) Finish() {
	m.nsq.Finish()
}

func (m *nsqMsg) Requeue(delay time.Duration) {
	m.nsq.RequeueWithoutBackoff(delay)
}

func NewNsqMessage(topic string, channel string, m *nsq.Message) Message {
	return &nsqMsg{
		topic:   topic,
		channel: channel,
		nsq:     m,
	}
}
