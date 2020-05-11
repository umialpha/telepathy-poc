#!/usr/bin/env python


from confluent_kafka import Producer, Consumer
import sys
import config
import logging
from metrics import profile

logger = logging.getLogger("ProducerClient")
logger.setLevel(logging.DEBUG)
logging.basicConfig(filename="ProducerClient")

class ProducerClient:

    def __init__(self):
        conf = {'bootstrap.servers': config.BOOTSTRAP_SERVER}
        self._producer = Producer(**conf)
        consumer_conf = {'bootstrap.servers': config.BOOTSTRAP_SERVER, 'session.timeout.ms': 6000,
            'auto.offset.reset': 'earliest'}
        self._consumer =  Consumer(consumer_conf)

    @profile(logger=logger)
    def submit_job(self, job):
        
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

        logger.debug('%% Waiting for %d deliveries\n' % len(p))
        self._producer.flush()
    
    @profile(logger=logger)
    def monitor_job(self, job):
        cnt = 0
        self._consumer.subscribe(config.JOB_FINISH_TOPIC + config.JOB_ID)
        while True:
            msg = self._consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise Exception(msg.error())
            else:
                cnt += 1
                if cnt >= len(job.tasks):
                    return

        



