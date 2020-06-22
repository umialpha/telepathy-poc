### POC V4
---

### Request
------------
Hi All,

Please check the following benchmark info,
1.	1M * 0.6 sec requests, 200KB each
2.	1 Broker (Standard_D32_v3, 32 cores) and 100 Compute Nodes (Standard_D4_v3, 4 cores)
3.	In theory, 1M * 0.6 sec / 400 / 60 = 25 minutes, warmup latency < 10 ms

### zkb pull model
---
dispatcher as a gRPC server, worker as gRPC client


### Benchmark
---
4core 8GB * 1 dispatcher
4core 8 GB * 18 dispatcher
workload: 100k * 600ms 
theory: 
  100k * 600ms / (4 * 18) = 83.333s
actual: 
  server: 85.22s cpu: (50/90/99 %ile): 8/10/13
  client: (50/90/99 %ile): 486.548µs/696.453µs/1.164481ms