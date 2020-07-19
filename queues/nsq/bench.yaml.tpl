apiVersion: batch/v1
kind: Job
metadata:
  name: bench-$ITEM
  labels:
    app: bench
spec:
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: bench
        image: r4aks.azurecr.io/nsq-bench:v0
        command: ["/go/bin/nsq-bench "]
        args: ["-runfor", "60s", "nsqd-address", "nsq", "-topic", "sub_bench_$ITEM", "-np", "10", "-ns", "10", "-nc", "1", "-flight", "1000", ]
        resources:
          requests:
            cpu: "3"
            memory: "4Gi"
