from threading import Lock, Thread
from collections import deque
import time

import grpc 
from concurrent import futures

from confluent_kafka import Consumer, KafkaException, Producer
import worker_pb2_grpc 
import worker_pb2
import config

TASK_RUNNING_TIME = 0.1

class WorkerSvc(worker_pb2_grpc.WorkerSvcServicer):

    def __init__(self):
        self._tasks = deque()
        self._lock = Lock()
        _running_task = Thread(target=self._run)
        _running_task.daemon = True
        _running_task.start()
        conf = {'bootstrap.servers': config.BOOTSTRAP_SERVER}
        self._producer = Producer(**conf)


    def _run(self):
        while True:
            task = None
            with self._lock:
                if self._tasks:
                    task = self._tasks.popleft()
            if not task:
                continue
            time.sleep(TASK_RUNNING_TIME)
            self._finish_task(task)

    
    def _finish_task(self, task):
        self._producer.produce(config.JOB_FINISH_TOPIC + config.JOB_ID, task)
        self._producer.poll(0)
        print("finish task", task)

    def send_task(self, request, context):
        taskid = request.taskid
        with self._lock:
            self._tasks.append(taskid)
        return worker_pb2.TaskResponse(taskid=taskid)

def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    worker_pb2_grpc.add_WorkerSvcServicer_to_server(WorkerSvc(), server)
    server.add_insecure_port('[::]:50053')
    server.start()
    server.wait_for_termination()


if __name__ == '__main__':
    
    serve()
    
