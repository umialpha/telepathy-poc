#!/usr/bin/env python
import time
import threading

from confluent_kafka import Producer, Consumer
import sys
import config
import logging
from metrics import profile


logger = logging.getLogger("producer")
logger.setLevel(logging.DEBUG)
logging.basicConfig(filename="producer.log")

class ProducerClient:

    def __init__(self):
        conf = {'bootstrap.servers': config.BOOTSTRAP_SERVER}
        self._producer = Producer(**conf)
        consumer_conf = {'bootstrap.servers': config.BOOTSTRAP_SERVER, 'session.timeout.ms': 6000,
            'auto.offset.reset': 'earliest', 'group.id': 1001,}
        self._consumer =  Consumer(consumer_conf)

    @profile(logger)
    def submit_job(self, job):
        logger.debug("submit job start " + str(job.jobid))
        def delivery_callback(err, msg):
            if err:
                print("{0} deliver error: {1}".format(msg, err))

        for task in job.tasks:
            try:
            # Produce line (without newline)
                self._producer.produce(config.JOB_SUBMIT_TOPIC + str(job.jobid), str(task.taskid), callback=delivery_callback)

            except BufferError:
                logger.debug('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                                len(self._producer))
            self._producer.poll(0)

        logger.debug('%% Waiting for %d deliveries\n' % len(self._producer))
        self._producer.flush(1)
    
    @profile(logger=logger)
    def monitor_job(self, job):
        cnt = 0
        self._consumer.subscribe(config.JOB_FINISH_TOPIC + str(config.JOB_ID))
        while True:
            msg = self._consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise Exception(msg.error())
            else:
                taskid = int(msg.value())
                logger.debug("task {0} cost {1} sec".format(taskid, time.time() - job.tasks[taskid].timestamp))

                cnt += 1
                if cnt >= len(job.tasks):
                    return

        

if __name__ == "__main__":
    from job import Job, Task

    def main_producer():
        job = Job(config.JOB_ID)
        for i in range(config.TASK_NUM):
            job.tasks.append(Task(i))
        
        p = ProducerClient()
        t1 = threading.Thread(target=p.submit_job, args=(job,))
        t1.start()
        p.monitor_job(job)

    main_producer()

