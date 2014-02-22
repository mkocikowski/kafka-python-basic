# -*- coding: UTF-8 -*-
# (c)2014 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/kafka-python-basic

import unittest
import logging

import kafka.connection
import kafka.client
import kafka.producer


class ProducerTest(unittest.TestCase):

    def test_init(self):
        producer = kafka.producer.KafkaProducer(hosts='192.168.44.11:9093,192.168.44.11:9094', topic='unittest01')        


    def test_send(self):
        producer = kafka.producer.KafkaProducer(hosts='192.168.44.11:9093,192.168.44.11:9094', topic='unittest01')        
        producer.send(['1', '2'])
        partitioned, response = producer.send(['3', '4', '5', '6', '7'])
        # this tests if messages are partitioned appropriately
        self.assertEqual(partitioned, [[kafka.protocol.Message(magic=0, attributes=0, key=None, value='5')], [kafka.protocol.Message(magic=0, attributes=0, key=None, value='6')], [kafka.protocol.Message(magic=0, attributes=0, key=None, value='3'), kafka.protocol.Message(magic=0, attributes=0, key=None, value='7')], [kafka.protocol.Message(magic=0, attributes=0, key=None, value='4')]])

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()

