package jobclient

import (
	"context"
	"os"
	"strconv"
	"sync"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/go-logr/logr"
	klogr "k8s.io/klog/v2/klogr"

	. "poc.telepathy.scale/pkg/config"
)

var logger logr.Logger = klogr.New().WithName("pulsar metric client")

func metricQueueName(jobid string) string {
	return jobid + "-metric"
}

type jobMetric struct {
	SumTime int32
	Cnt     int32
}

type collector struct {
	metric jobMetric
	jobid  string
	// client pulsar.Client
	// reader pulsar.Reader
	stopCh chan bool
}

func (c *collector) collectMetricForJob(ctx context.Context) {
	pulsarURL := os.Getenv(PULSAR_ADDR)
	client, err := pulsar.NewClient(pulsar.ClientOptions{URL: pulsarURL})
	if err != nil {
		logger.Error(err, "Init Pulsar client error")
		return
	}

	defer client.Close()

	reader, err := client.CreateReader(pulsar.ReaderOptions{
		Topic:          metricQueueName(c.jobid),
		StartMessageID: pulsar.EarliestMessageID(),
	})
	if err != nil {
		logger.Error(err, "Init Pulsar reader error")
		return
	}
	defer reader.Close()
	for {

		msg, err := reader.Next(ctx)
		if err != nil {
			if err == context.Canceled {
				return
			}
			logger.Error(err, "reader.Next error")
			continue
		}
		c.parseMsg(msg)
	}

}

func (c *collector) parseMsg(msg pulsar.Message) {
	exeTime, err := strconv.Atoi(string(msg.Payload()))
	if err != nil {
		logger.Error(err, "Parse Pulsar Metric Error, MSG: ", string(msg.Payload()))
	}
	c.metric.Cnt++
	c.metric.SumTime += int32(exeTime)
}

func newJobCollector(jobid string, stopCh chan bool) *collector {
	c := &collector{
		metric: jobMetric{SumTime: 0, Cnt: 0},
		jobid:  jobid,
		stopCh: stopCh,
	}
	ctx, cancel := context.WithCancel(context.Background())
	go c.collectMetricForJob(ctx)
	go func() {
		<-c.stopCh
		cancel()
	}()
	return c
}

type pulsarJobClient struct {
	jobCollectors map[string]*collector
	jcMu          sync.Mutex
	stopCh        chan bool
}

func (s *pulsarJobClient) GetAverageExeutionMS(jobid string) int32 {
	s.jcMu.Lock()
	defer s.jcMu.Unlock()
	c, ok := s.jobCollectors[jobid]
	if !ok {
		s.jobCollectors[jobid] = newJobCollector(jobid, s.stopCh)
		return -1
	}

	return int32(float32(c.metric.SumTime) / (float32(c.metric.Cnt) + 0.01))
}

func (s *pulsarJobClient) Stop() {
	s.stopCh <- true
	return
}

func NewPulsarJobClient() IJobClient {
	c := &pulsarJobClient{
		jobCollectors: make(map[string]*collector),
		stopCh:        make(chan bool),
	}
	return c
}
