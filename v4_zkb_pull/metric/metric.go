package metric

import (
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"time"

	"github.com/shirou/gopsutil/cpu"
	"google.golang.org/grpc/benchmark/stats"
)

var hopts = stats.HistogramOptions{
	NumBuckets:   2495,
	GrowthFactor: .01,
}

type Histogram = stats.Histogram

// func GetCPUTime() int64 {
// 	var ts unix.Timespec
// 	if err := unix.ClockGettime(unix.CLOCK_PROCESS_CPUTIME_ID, &ts); err != nil {
// 		fmt.Println(err)
// 		return 0
// 	}
// 	return ts.Nano()
// }

func ParseHist(hist *stats.Histogram) {
	fmt.Printf("(50/90/99 %%ile): %v/%v/%v\n",
		Median(.5, hist),
		Median(.9, hist),
		Median(.99, hist))
}

func Median(percentile float64, h *stats.Histogram) int64 {
	need := int64(float64(h.Count) * percentile)
	have := int64(0)
	for _, bucket := range h.Buckets {
		count := bucket.Count
		if have+count >= need {
			percent := float64(need-have) / float64(count)
			return int64((1.0-percent)*bucket.LowBound + percent*bucket.LowBound*(1.0+hopts.GrowthFactor))
		}
		have += bucket.Count
	}
	panic("should have found a bound")
}

func NewHistogram() *stats.Histogram {
	return stats.NewHistogram(hopts)
}

type Bench struct {
	isStart bool
	mu      sync.RWMutex

	startTime time.Time
	endTime   time.Time

	cpuStartTime int64
	cpuEndTime   int64

	testName string
	cpuFile  *os.File
	memFile  *os.File

	cpuTick int
	hist    *stats.Histogram
	done    chan bool
}

func (b *Bench) Start() {
	if b.isStart {
		return
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.isStart {
		return
	}
	b.isStart = true
	b.startTime = time.Now()
	//b.cpuStartTime = GetCPUTime()

	// pprof cpu
	if b.testName != "" {
		var err error
		b.cpuFile, err = os.Create("/tmp/" + b.testName + ".cpu")
		if err != nil {
			fmt.Printf("Error creating file: %v", err)
			b.cpuFile.Close()
		}
		pprof.StartCPUProfile(b.cpuFile)

	}

	// cpu sampling
	if b.cpuTick != 0 {
		go b.samplingCPU()
	}
}

func (b *Bench) End() {
	b.mu.RLock()
	defer b.mu.RUnlock()
	b.endTime = time.Now()
	//b.cpuEndTime = GetCPUTime()
	b.done <- true
	// pprof
	pprof.StopCPUProfile()
	b.cpuFile.Close()
	if b.testName != "" {
		mf, err := os.Create("/tmp/" + b.testName + ".mem")
		defer mf.Close()
		if err != nil {
			fmt.Printf("Error creating file: %v", err)
			return
		}
		runtime.GC() // materialize all statistics
		if err := pprof.WriteHeapProfile(mf); err != nil {
			fmt.Printf("Error writing memory profile: %v", err)
		}
	}

}

func (b *Bench) samplingCPU() {
	b.hist = NewHistogram()
	c := time.Tick(time.Duration(b.cpuTick) * time.Millisecond)
	sampling := true
	for sampling {
		select {
		case <-c:
			usages, err := cpu.Percent(0, false)
			if err != nil {
				fmt.Println("Get CPU Percent failed", err)
				return
			}
			//fmt.Println("cpu percent", int64(usages[0]))
			b.hist.Add(int64(usages[0]))
			break
		case <-b.done:
			sampling = false
			fmt.Println("END Sampling")
			break
		}
	}

}
func (b *Bench) Duration() time.Duration {
	return b.endTime.Sub(b.startTime)
}

// func (b *Bench) CPUDuration() time.Duration {
// 	return time.Duration(b.cpuEndTime - b.cpuStartTime)
// }

func (b *Bench) CPUUsage() {
	if b.hist != nil {
		ParseHist(b.hist)
	}

}

func NewBench(testName string, cpuTick int) *Bench {
	bench := &Bench{
		testName: testName,
		cpuTick:  cpuTick,
		done:     make(chan bool, 1),
	}
	return bench
}
