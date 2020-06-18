package main

import "flag"

var (
	addr        = flag.String("b", "bootstrap.kafka.svc.cluster.local:9092", "broker address")
	topic       = flag.String("t", "bench-test", "topic")
	msgSize     = flag.Int("m", 1, "message size in byte")
	mode        = flag.String("mode", "p", "test mode, c / p")
	warmDur     = flag.Int("w", 10, "warm up duration in seconds")
	numMessages = flag.Int("n", 1000000, "message num")
)
