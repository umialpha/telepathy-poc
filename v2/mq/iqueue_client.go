package mq

type IQueueClient interface {
	CreateQueue(queuName string, a ...interface{}) error
	Produce(queueName string, value []byte, opt ...interface{}) error
	Consume(queueName string, abort <-chan int, opt ...interface{}) (<-chan []byte, <-chan error)
}
