package mq

type IQueueClient interface {
	CreateQueue(queuName string, a ...interface{}) error
	Produce(queueName string, key interface{}, value interface{}, opt ...interface{}) error
	Consume(queueName string, opt ...interface{}) (interface{}, error)
}
