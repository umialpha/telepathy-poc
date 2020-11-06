package queueclient

import (
	"os"

	"github.com/go-logr/logr"
	klogr "k8s.io/klog/v2/klogr"

	. "poc.telepathy.scale/pkg/config"
)

var logger logr.Logger = klogr.New().WithName("pulsar queue client")

type pulsarQueueClient struct {
	IQueueClient
}

func (c *pulsarQueueClient) IsQueueExist(qn string) bool {
	url := os.Getenv(PULSAR_ADMIN_ADDR) + "/admin/v2/persistent/public/default/" + qn + "/stats"
	logger.Info(url)

	err := httpGet(url, nil)
	if err != nil {
		logger.Error(err, "IsQueueExist error", qn)
		return false
	}
	return true

}

func (c *pulsarQueueClient) GetQueueLength(qn string) int32 {
	url := os.Getenv(PULSAR_ADMIN_ADDR) + "/admin/v2/persistent/public/default/" + qn + "/stats"
	p := PulsarTopicStats{}
	err := httpGet(url, &p)
	if err != nil {
		logger.Error(err, "GetQueueLength", qn)
		return 0
	}
	if _, ok := p.Subscriptions[qn]; !ok {
		logger.Info("No subscriptions for Topic", "Queue Name", qn)
		return p.MsgInCounter
	}
	return p.Subscriptions[qn].MsgBacklog

}

func NewPulsarQueueClient() IQueueClient {
	return &pulsarQueueClient{}

}
