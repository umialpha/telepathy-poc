---
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
        imagePullPolicy: Always
        command: ["/go/bin/nsq.bench"]
        args: ["-runfor", "60s", "-nsqd-address", "nsqd:4150", "-lookup-addresses", "nsqlookupd-0.nsqlookupd:4161 nsqlookupd-1.nsqlookupd:4161 nsqlookupd-2.nsqlookupd:4161", "-topic", "sub_bench_$ITEM", "-np", "10", "-ns", "10", "-nc", "1", "-flight", "200", ]
        resources:
          requests:
            cpu: "2"
            memory: "4Gi"

      - name: nsqd
        image: nsqio/nsq:v1.1.0
        imagePullPolicy: Always
        resources:
          requests:
            cpu: 2
            memory: 1Gi
        ports:
        - containerPort: 4150
          name: tcp
        - containerPort: 4151
          name: http
        livenessProbe:
          httpGet:
            path: /ping
            port: http
          initialDelaySeconds: 5
        readinessProbe:
          httpGet:
            path: /ping
            port: http
          initialDelaySeconds: 2
        volumeMounts:
        - name: datadir
          mountPath: /data
        command:
          - /nsqd
          - -data-path
          - /data
          - -lookupd-tcp-address
          - nsqlookupd-0.nsqlookupd:4160
          - -lookupd-tcp-address
          - nsqlookupd-1.nsqlookupd:4160
          - -lookupd-tcp-address
          - nsqlookupd-2.nsqlookupd:4160
          - -broadcast-address
          - $(HOSTNAME).nsqd
          - -mem-queue-size
          - "1000000"
        env:
        - name: HOSTNAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
      terminationGracePeriodSeconds: 5
      volumes:
      - name: datadir
        persistentVolumeClaim:
          claimName: datadir-$ITEM    
---

apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: datadir-$ITEM
spec:
  accessModes:
    - "ReadWriteOnce"
  storageClassName: managed-premium
  resources:
    requests:
      storage: 50Gi

