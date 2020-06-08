package mq

type IQueueClient interface {
	CreateQueues(queuName []string, a ...interface{}) error
	Produce(queueName string, value []byte, opt ...interface{}) error
	Consume(queueName string, groupID string, abort <-chan int, opt ...interface{}) (<-chan []byte, <-chan error)
	DeleteQueues(queueName []string, a ...interface{}) error
}
