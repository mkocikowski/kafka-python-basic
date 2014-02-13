# -*- coding: UTF-8 -*-
# (c)2013 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/esbench

import unittest
import logging

import kafka.client
import kafka.consumer


class ConsumerTest(unittest.TestCase):

    def setUp(self):
        self.client = kafka.client.KafkaClient("192.168.44.11:9093,192.168.44.11:9094")

    def tearDown(self):
        self.client.close()

    def test_init(self):
        consumer = kafka.consumer.Consumer(self.client, 'g1', 'topic01')

    def test_fetch(self):
        consumer = kafka.consumer.Consumer(self.client, 'g1', 'topic01')
        consumer.fetch()


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()

