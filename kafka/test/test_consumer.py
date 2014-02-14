# -*- coding: UTF-8 -*-
# (c)2014 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/kafka-python-basic

import unittest
import logging

import kafka.connection
import kafka.client
import kafka.consumer


class ConsumerTest(unittest.TestCase):

#     def setUp(self):
#         self.client = kafka.client.KafkaClient("192.168.44.11:9093,192.168.44.11:9094")

#     def tearDown(self):
#         self.client.close()

    def test_init(self):
        consumer = kafka.consumer.KafkaConsumer("192.168.44.11:9093,192.168.44.11:9094", None, 'unittest01')


    def test_fetch(self):
        consumer = kafka.consumer.KafkaConsumer("192.168.44.11:9093,192.168.44.11:9094", None, 'unittest01')
        messages = consumer.fetch()
        self.assertTrue("foo" in messages)
        self.assertTrue("bar" in messages)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()

