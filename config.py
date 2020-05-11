BOOTSTRAP_SERVER = "172.16.0.5:9092"
JOB_SUBMIT_TOPIC = "kafka-poc-job-submit-"
JOB_FINISH_TOPIC = "kafka-poc-job-finish-"
JOB_ID = 1011
TASK_NUM = 10**6 
WORKERS_ADDRS = ["172.16.0.8:{0}".format(i) for i in range(5001, 5001 + 32)]
