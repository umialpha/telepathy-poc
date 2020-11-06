package queueclient

type IQueueClient interface {
	IsQueueExist(string) bool
	GetQueueLength(string) int32
}
