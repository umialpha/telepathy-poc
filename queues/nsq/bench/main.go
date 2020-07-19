package main

import (
	"flag"
	"log"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nsqio/go-nsq"
)

var (
	runfor            = flag.Duration("runfor", 10*time.Second, "duration of time to run, e.g `1h1m10s`, `10ms`")
	nsqdAddr          = flag.String("nsqd-address", "127.0.0.1:4150", "<addr>:<port> to connect to nsqd")
	lookupAddrs       = flag.String("lookup-addresses", "127.0.0.1:4161", "`<addr1>:<port1> <addr2>:<port2>` to connect to nsqd")
	topic             = flag.String("topic", "sub_bench", "topic to receive messages on")
	channel           = flag.String("channel", "ch", "channel to receive messages on")
	size              = flag.Int("size", 200, "size of messages")
	batchSize         = flag.Int("batch-size", 200, "batch size of messages")
	deadline          = flag.String("deadline", "", "deadline to start the benchmark run")
	numPubs           = flag.Int("np", 10, "Number of concurrent publishers")
	numSubs           = flag.Int("ns", 0, "Number of concurrent subscribers")
	numSubConcurrency = flag.Int("nc", 1, "Number of concurrent handlers each subscribe has ")
	maxInFlight       = flag.Int("flight", 200, "max messages in flight")
)

var totalPubMsgCount int64
var totalRecvMsgCount int64

func pubWorker(td time.Duration, addr string, batchSize int, batch [][]byte, topic string, rdy *sync.WaitGroup, startCh <-chan int) {
	config := nsq.NewConfig()
	producer, err := nsq.NewProducer(addr, config)
	if err != nil {
		panic(err.Error())
	}
	// ready and wait to start
	rdy.Done()
	<-startCh

	var msgCount int64
	endTime := time.Now().Add(td)
	for {
		err := producer.MultiPublish(topic, batch)
		if err != nil {
			panic(err.Error())
		}
		msgCount += int64(batchSize)
		if time.Now().After(endTime) {
			break
		}
	}
	atomic.AddInt64(&totalPubMsgCount, msgCount)
}

type myHandler struct {
	MsgCount int64
}

func (h *myHandler) HandleMessage(m *nsq.Message) error {
	if len(m.Body) == 0 {
		// Returning nil will automatically send a FIN command to NSQ to mark the message as processed.
		return nil
	}
	if *numSubConcurrency == 1 {
		h.MsgCount++
	} else {
		atomic.AddInt64(&h.MsgCount, 1)
	}

	// Returning a non-nil error will automatically send a REQ command to NSQ to re-queue the message.
	return nil
}

func subWorker(td time.Duration, tcpAddrs []string, topic string, channel string, rdy *sync.WaitGroup, startCh <-chan int) {
	config := nsq.NewConfig()
	config.MaxInFlight = *maxInFlight
	consumer, err := nsq.NewConsumer(topic, channel, config)
	if err != nil {
		panic(err.Error())
	}
	h := &myHandler{}
	if *numSubConcurrency == 1 {
		consumer.AddHandler(h)
	} else {
		consumer.AddConcurrentHandlers(h, *numSubConcurrency)

	}

	// ready and wait to start
	rdy.Done()
	<-startCh

	err = consumer.ConnectToNSQLookupds(tcpAddrs)
	if err != nil {
		log.Fatal(err)
	}
	time.Sleep(td)
	consumer.Stop()

	atomic.AddInt64(&totalRecvMsgCount, h.MsgCount)
}

func main() {

	// construct msg
	msg := make([]byte, *size)
	batch := make([][]byte, *batchSize)
	for i := range batch {
		batch[i] = msg
	}

	flag.Parse()

	var wg sync.WaitGroup
	var pubRdy sync.WaitGroup
	var subRdy sync.WaitGroup
	pubStCh := make(chan int)
	subStCh := make(chan int)
	for i := 0; i < *numPubs; i++ {
		wg.Add(1)
		pubRdy.Add(1)
		go func() {
			pubWorker(*runfor, *nsqdAddr, *batchSize, batch, *topic, &pubRdy, pubStCh)
			wg.Done()
		}()
	}
	pubRdy.Wait()

	for j := 0; j < *numSubs; j++ {
		wg.Add(1)
		subRdy.Add(1)
		go func() {
			subWorker(*runfor, strings.Split(*lookupAddrs, " "), *topic, *channel, &subRdy, subStCh)
			wg.Done()
		}()
	}
	subRdy.Wait()

	start := time.Now()
	close(pubStCh)
	close(subStCh)
	wg.Wait()
	end := time.Now()
	duration := end.Sub(start)
	tmc := atomic.LoadInt64(&totalPubMsgCount)
	log.Printf("[Publisher] duration: %s - %.03fmb/s - %.03fops/s - %.03fus/op",
		duration,
		float64(tmc*int64(*size))/duration.Seconds()/1024/1024,
		float64(tmc)/duration.Seconds(),
		float64(duration/time.Microsecond)/float64(tmc))

	tmc = atomic.LoadInt64(&totalRecvMsgCount)
	log.Printf("[Consumer] duration: %s - %.03fmb/s - %.03fops/s - %.03fus/op",
		duration,
		float64(tmc*int64(*size))/duration.Seconds()/1024/1024,
		float64(tmc)/duration.Seconds(),
		float64(duration/time.Microsecond)/float64(tmc))

}
