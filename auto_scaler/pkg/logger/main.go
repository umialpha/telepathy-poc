package main

import (
	"flag"

	"github.com/go-logr/glogr"
)

type E struct {
	str string
}

func (e E) Error() string {
	return e.str
}

func main() {
	flag.Set("v", "1")
	flag.Set("logtostderr", "true")
	flag.Parse()
	log := glogr.New().WithName("MyName")
	log.Info("hello", "val1", 1, "val2", map[string]int{"k": 1})
	log.V(1).Info("you should see this")
	log.V(1).V(1).Info("you should NOT see this")
	log.Error(nil, "uh oh", "trouble", true, "reasons", []float64{0.1, 0.11, 3.14})
	log.Error(E{"an error occurred"}, "goodbye", "code", -1)
	//glog.Flush()
}
