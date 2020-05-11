#!/usr/bin/env python
#
# Copyright 2016 Confluent Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#
# Example high-level Kafka 0.9 balanced Consumer
#
from confluent_kafka import Consumer, KafkaException, Producer
import sys
import getopt
import json
import logging
from policies import random_pick
import config
from rpc.worker_client import WorkerClient
from metrics import profile


logger = logging.getLogger("consumer")
logger.setLevel(logging.DEBUG)
logging.basicConfig(filename="consumer.log", filemode="w")

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
        logger.debug("start to consume {}".format(topics))
        self._consumer.subscribe(topics)
        while True:
            msg = self._consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                self.close_workers()
                raise KafkaException(msg.error())
            else:
               self._dispatch(msg)

    def _dispatch(self, msg):
        # self.finish_task(msg)
        worker = self._select_best_worker()
        if not worker:
            raise Exception("No Workers")
        try:        
            worker.send_task(msg)
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

    