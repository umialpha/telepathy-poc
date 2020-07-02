package mq

type DummyQueue struct {
	IQueueClient
}

func (q *DummyQueue) CreateQueue(queueName string) error {
	return nil
}

// Put(interface{}) error
// Get(interface{}) error
// Dispose() error
