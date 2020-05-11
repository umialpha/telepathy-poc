from threading import Lock, Thread
import queue
from collections import deque
import time
import logging
import os

import grpc 
from concurrent import futures

from confluent_kafka import Consumer, KafkaException, Producer
import rpc.worker_pb2_grpc as worker_pb2_grpc
import rpc.worker_pb2 as worker_pb2
import config

TASK_RUNNING_TIME = 0
FORMAT = '%(asctime)-15s %(message)s'
logger = logging.getLogger("worker")
logger.setLevel(logging.INFO)
logging.basicConfig(filename="worker-{0}.log".format(os.getpid()), filemode="w", format=FORMAT)

class WorkerSvc(worker_pb2_grpc.WorkerSvcServicer):

    def __init__(self):
        self._tasks = queue.Queue()
        _running_task = Thread(target=self._run)
        _running_task.daemon = True
        _running_task.start()
        conf = {'bootstrap.servers': config.BOOTSTRAP_SERVER}
        self._producer = Producer(**conf)
        # self._finished_num = 0
        


    def _run(self):
        while True:
            task = self._tasks.get()
            # time.sleep(TASK_RUNNING_TIME)
            self._finish_task(task)

    
    def _finish_task(self, task):
        self._producer.produce(config.JOB_FINISH_TOPIC + str(config.JOB_ID), str(task), timestamp=int(time.time()))
        self._producer.poll(0)
        # logger.debug("finish task " + str(task))
        # self._finished_num += 1
        # if self._finished_num % 1000 == 0:
        #     logger.info("finished tasks " + str(self._finished_num))

    def send_task(self, request_iterator, context):
        for request in request_iterator:
            taskid = request.taskid
            self._tasks.put(taskid, False)
            yield worker_pb2.TaskResponse(taskid=taskid)


    
