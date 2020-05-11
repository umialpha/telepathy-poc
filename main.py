from concurrent import futures

import grpc

from rpc.worker_svc import WorkerSvc
import rpc.worker_pb2_grpc as worker_pb2_grpc
import rpc.worker_pb2
import config
from job import Job, Task
from producer import ProducerClient
from consumer import ConsumerClient

def main_producer():
    
    job = Job(config.JOB_ID)
    for i in range(config.TASK_NUM):
        job.tasks.append(Task(i))
    
    p = ProducerClient()
    p.submit_job(job)


def main_consumer():
    consumer = ConsumerClient()
    consumer.consume([config.JOB_SUBMIT_TOPIC + config.JOB_ID])

def main_worker(port):
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    worker_pb2_grpc.add_WorkerSvcServicer_to_server(WorkerSvc(), server)
    server.add_insecure_port('[::]:{0}'.format(port))
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    import sys
    main_type = sys.argv[1]
    if main_type == "p":
        main_producer()
    elif main_type == 'c':
        main_consumer()
    elif main_type == 'w':
        main_worker(sys.argv[2])
