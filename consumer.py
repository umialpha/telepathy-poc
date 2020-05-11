#!/usr/bin/env python
#


import time
from confluent_kafka import Consumer, KafkaException, Producer
import sys
import getopt
import json
import logging
from policies import random_pick
import config
from rpc.worker_client import WorkerClient
from metrics import profile

FORMAT = '%(asctime)-15s %(message)s'
logger = logging.getLogger("consumer")
logger.setLevel(logging.INFO)
logging.basicConfig(filename="consumer.log", filemode="w", format=FORMAT)

def delivery_callback(err, msg):
    if err:
        print("{0} deliver error: {1}".format(msg, err))


class ConsumerClient:

    def __init__(self):
        conf = {'bootstrap.servers': config.BOOTSTRAP_SERVER, 'session.timeout.ms': 6000,
            'auto.offset.reset': 'earliest', 'group.id': 1001,}
        self._consumer =  Consumer(conf)
        self._workers = []
        self._init_workers()


    def _init_workers(self):
        for endpoint in config.WORKERS_ADDRS:
            self._workers.append(WorkerClient(endpoint))

    def close_workers(self):
        for w in self._workers:
            w.close()
    

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
                self.close_workers()
                raise KafkaException(msg.error())
            else:
                if cnt % 1000 == 0:
                    logger.info("{0} receive tasks cost {1} sec".format(cnt, time.time() - now))
                self._dispatch(msg)

    def _dispatch(self, msg):
        # self.finish_task(msg)
        worker = self._select_best_worker()
        if not worker:
            raise Exception("No Workers")
        try:        
            worker.send_task(msg.value())
        except Exception as e:
            logger.debug("send task error,{0}, {1}".format(e, msg))
            self.close_workers()
            raise

    def _select_best_worker(self):
        if not self._workers:
            return None
        return random_pick(self._workers)



if __name__ == "__main__":

    def main_consumer():
        consumer = ConsumerClient()
        consumer.consume([config.JOB_SUBMIT_TOPIC + str(config.JOB_ID)])
    
    main_consumer()

    