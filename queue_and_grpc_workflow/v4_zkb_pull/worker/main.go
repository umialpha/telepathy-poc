package main

import (
	"context"
	"flag"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc"
	"t.poc.v4/metric"
	pb "t.poc.v4/protos"
)

var (
	taskCost     = flag.Int("c", 60, "task calculationo time in millisecond")
	taskParallel = flag.Int("p", 4, "task in parallel")
	svcAddr      = flag.String("s", "localhost:4002", "dispatcher address")
	testName     = flag.String("test_name", "", "Name of the test used for creating profiles.")
	cpuTick      = flag.Int("cpu_tick", 0, "if set, it will sampling cpu usage in 'cpu_tick' millisecond")
	rqSize       = flag.Int("req", 1, "Request message size in bytes.")
	rspSize      = flag.Int("resp", 1, "Response message size in bytes.")
)

type Worker struct {
	capChan  chan int
	wg       sync.WaitGroup
	done     chan bool
	client   pb.DispatcherSvrClient
	bench    *metric.Bench
	hist     *metric.Histogram
	histLock sync.Mutex
}

func (w *Worker) Run() {

	w.bench.Start()
	running := true
	for running {
		select {
		case <-w.done:
			running = false
			break
		case <-w.capChan:
			go w.doWork()
		}
	}
	w.wg.Wait()
	fmt.Println("END Run")
	w.bench.End()
	fmt.Printf("duration: %v", w.bench.Duration())
	w.bench.CPUUsage()
	fmt.Printf("(50/90/99 %%ile): %v/%v/%v\n",
		time.Duration(metric.Median(.5, w.hist)),
		time.Duration(metric.Median(.9, w.hist)),
		time.Duration(metric.Median(.99, w.hist)))

}

func (w *Worker) doWork() {
	w.wg.Add(1)
	defer func() {
		w.wg.Done()
		w.capChan <- 1
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	st := time.Now()
	resp, err := w.client.ReqTask(ctx, &pb.TaskRequest{TaskId: 0, Body: make([]byte, *rqSize)})
	rElapsed := time.Since(st).Nanoseconds()

	if err != nil {
		fmt.Println("ReqTask Err", err)
		return
	}
	// task 0 means it is still warm-up time
	if resp.TaskId == 0 {
		time.Sleep(100 * time.Millisecond)
		return
	}
	// task -1 means there is no task
	if resp.TaskId < 0 {
		w.done <- true
		return
	}
	time.Sleep(time.Duration(*taskCost) * time.Millisecond)
	st = time.Now()
	w.client.FinTask(context.Background(), &pb.TaskRequest{TaskId: resp.TaskId, Body: make([]byte, *rqSize)})
	fElasped := time.Since(st).Nanoseconds()
	w.histLock.Lock()
	defer w.histLock.Unlock()
	w.hist.Add(rElapsed)
	w.hist.Add(fElasped)
	return

}

func NewWorker() *Worker {
	w := &Worker{}
	w.capChan = make(chan int, *taskParallel)
	w.done = make(chan bool, *taskParallel)
	for i := 0; i < *taskParallel; i++ {
		w.capChan <- 1
	}
	conn, err := grpc.Dial(*svcAddr, grpc.WithInsecure())
	if err != nil {
		fmt.Printf("fail to dial: %v\n", err)
		return nil
	}
	w.client = pb.NewDispatcherSvrClient(conn)
	w.bench = metric.NewBench(*testName, *cpuTick)
	w.hist = metric.NewHistogram()
	return w
}

func main() {
	forever := make(chan int)
	flag.Parse()
	w := NewWorker()
	go w.Run()
	<-forever
}
