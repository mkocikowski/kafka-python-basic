# -*- coding: UTF-8 -*-
# (c)2013 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/esbench

import unittest
import logging

import kafka.connection
import kafka.client
import kafka.consumer


class ConsumerTest(unittest.TestCase):

    def setUp(self):
#         self.tmp_sock_send = kafka.connection.KafkaConnection.send
#         kafka.connection.KafkaConnection.send = lambda x: true
#         self.tmp_sock_recv = kafka.connection.KafkaConnection.recv
        self.client = kafka.client.KafkaClient("192.168.44.11:9093,192.168.44.11:9094")
#
    def tearDown(self):
        self.client.close()
#         kafka.connection.KafkaConnection.send = self.tmp_sock_send
#         kafka.connection.KafkaConnection.recv = self.tmp_sock_recv

    def test_init(self):
        consumer = kafka.consumer.KafkaConsumer(self.client, 'g1', 'topic01')


    def test_fetch(self):
        consumer = kafka.consumer.KafkaConsumer(self.client, 'g1', 'topic01')
        messages = consumer.fetch()
        print(messages)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()

