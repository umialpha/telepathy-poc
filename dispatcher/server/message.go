package server

import (
	"encoding"
	"encoding/json"
	"time"
)

type MessageID string

func (a MessageID) String() string {
	return string(a)
}

type Message interface {
	encoding.BinaryMarshaler
	encoding.BinaryUnmarshaler
	GetID() MessageID
	GetPayload() []byte
	IsExpired() bool
	ExpiredTime() time.Time
	Touch()
	Finish()
	Requeue(time.Duration)
}

type message struct {
	Message

	ID         string
	Payload    []byte
	ExpireTime int64
}

func (m *message) GetID() MessageID {
	return MessageID(m.ID)
}

func (m *message) GetPayload() []byte {
	return m.Payload
}

func (m *message) IsExpired() bool {
	return m.ExpireTime <= 0
}

func (m *message) ExpiredTime() time.Time {
	return time.Unix(m.ExpireTime, 0)
}

func (m *message) Touch() {
}

func (m *message) Finish() {
}

func (m *message) Requeue(delay time.Duration) {
}

// TODO, replace json with more efficent marshaller
func (m *message) MarshalBinary() (data []byte, err error) {
	return json.Marshal(m)

}

func (m *message) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, m)

}

func NewMessage(id string, payload []byte, expireTime int64) Message {
	msg := &message{
		ID:         id,
		Payload:    payload,
		ExpireTime: expireTime,
	}

	return msg
}
