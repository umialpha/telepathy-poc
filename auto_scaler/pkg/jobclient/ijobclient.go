package jobclient

// import (
// )

type IJobClient interface {
	GetAverageExeutionMS(string) int32
	Stop()
}
