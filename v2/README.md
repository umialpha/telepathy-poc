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