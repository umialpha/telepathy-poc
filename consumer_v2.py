#!/usr/bin/env python
#


import time
import os
from confluent_kafka import Consumer, KafkaException, Producer
import sys
import getopt
import json
import logging
# from policies import random_pick
import config
from rpc.worker_client import WorkerClient
from metrics import profile

FORMAT = '%(asctime)-15s %(message)s'
logger = logging.getLogger("consumer")
logger.setLevel(logging.INFO)
logging.basicConfig(filename="consumer-{0}.log".format(os.getpid()), filemode="w", format=FORMAT)

def delivery_callback(err, msg):
    if err:
        print("{0} deliver error: {1}".format(msg, err))


class ConsumerClient:

    def __init__(self):
        conf = {'bootstrap.servers': config.BOOTSTRAP_SERVER, 'session.timeout.ms': 6000,
            'auto.offset.reset': 'earliest', 'group.id': 1001,}
        self._consumer =  Consumer(conf)
        conf = {'bootstrap.servers': config.BOOTSTRAP_SERVER}
        self._producer = Producer(**conf)


    
    

    @profile(logger=logger)
    def consume(self, topics):
        logger.info("start to consume {}".format(topics))
        now = time.time()
        cnt = 0
        self._consumer.subscribe(topics)
        while True:
            msg = self._consumer.poll(timeout=1.0)
            if msg is None:
                continue
            cnt += 1
            if msg.error():
                
                raise KafkaException(msg.error())
            else:
                if cnt % 10000 == 0:
                    logger.info("{0} receive tasks cost {1} sec".format(cnt, time.time() - now))
                self._consume(msg)

    
    def _consume(self, msg):
        time.sleep(0.01)
        self._producer.produce(config.JOB_FINISH_TOPIC + str(config.JOB_ID), str(msg.value()), timestamp=int(time.time()))
        self._producer.poll(0)

   
        



if __name__ == "__main__":

    def main_consumer():
        consumer = ConsumerClient()
        consumer.consume([config.JOB_SUBMIT_TOPIC + str(config.JOB_ID)])
    
    main_consumer()

    