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
        consumer = kafka.consumer.KafkaConsumer(hosts='192.168.44.11:9093,192.168.44.11:9094', topic='unittest01')        
        self.assertEqual(sorted(consumer.__dict__.keys()), 
            ['client', 'failfast', 'group', 'hosts', 'offsets', 
             'offsets_file_path', 'offsets_pending', 'topic', 'whence'])

    def test_seek(self):
        consumer = kafka.consumer.KafkaConsumer(hosts='192.168.44.11:9093,192.168.44.11:9094', topic='unittest01')
        self.assertEqual(consumer.offsets, {})
        self.assertEqual(consumer.whence, kafka.consumer.WHENCE_SAVED)
        consumer.seek()
        self.assertEqual(consumer.offsets, {0: 0, 1: 0, 2: 0, 3: 0})
        consumer.whence = kafka.consumer.WHENCE_TAIL
        consumer.seek()
        self.assertNotEqual(consumer.offsets, {0: 0, 1: 0, 2: 0, 3: 0})
        

    def test_fetch(self):
        consumer = kafka.consumer.KafkaConsumer(hosts='192.168.44.11:9093,192.168.44.11:9094', topic='unittest01')
        messages = consumer.fetch()
        self.assertTrue("foo" in messages)
        self.assertTrue("bar" in messages)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()

