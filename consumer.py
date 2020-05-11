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

def delivery_callback(err, msg):
    if err:
        print("{0} deliver error: {1}".format(msg, err))


class ConsumerClient:

    def __init__(self):
        conf = {'bootstrap.servers': config.BOOTSTRAP_SERVER, 'session.timeout.ms': 6000,
            'auto.offset.reset': 'earliest'}
        self._consumer =  Consumer(conf)
        conf = {'bootstrap.servers': config.BOOTSTRAP_SERVER}
        self._producer = Producer(**conf)

        self._workers = []

    def add_worker(self, worker):
        self._workers.append(worker)


    def consume(self, topics):
        self._consumer.subscribe(topics)
        while True:
            msg = self._consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            else:
               self._dispatch(msg)

    def _dispatch(self, msg):
        self.finish_task(msg)
        # worker = self._select_best_worker()
        # if not worker:
        #     raise Exception("No Workers")
        # try:        
        #     worker.send_task(msg)
        # except Exception as e:
        #     print("send task error,", e, msg)


    def _select_best_worker(self):
        if not self._workers:
            return None
        return random_pick(self._workers)

    def finish_task(self, task):
        self._producer.produce(config.JOB_FINISH_TOPIC + config.JOB_ID, task, callback=delivery_callback)
        self._producer.poll(0)