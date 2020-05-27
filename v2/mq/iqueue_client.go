package mq

type IQueueClient interface {
	CreateQueue(queuName string, a ...interface{}) error
	Produce(queueName string, a ...interface{}) error
	Consume(queueName string, a ...interface{}) (interface{}, error)
}
