# -*- coding: UTF-8 -*-
# (c)2014 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/kafka-python-basic

import unittest
import logging
import base64

import kafka.protocol
import kafka.connection
import kafka.client
import kafka.test.test_connection


class ClientTest(unittest.TestCase):

    def test_init(self):
        client = kafka.client.KafkaClient("192.168.33.10:9092")
        self.assertEqual(sorted(client.conns), [kafka.connection.KafkaConnection('192.168.33.10', 9092)])

    def test_get_metadata(self):
        client = kafka.client.KafkaClient("192.168.33.10:9092")
        client.get_metadata()
        self.assertTrue(kafka.protocol.TopicAndPartition(topic='unittest1', partition=0) in client.topics_to_brokers)

    def test_fetch(self):
        client = kafka.client.KafkaClient("192.168.33.10:9092")
        data = [m.message.value for p in [client.fetch("unittest1", n, 0) for n in range(4)] for m in p]
        self.assertTrue('foo' in data)
        self.assertTrue('bar' in data)

    def test_get_offsets(self):
        client = kafka.client.KafkaClient("192.168.33.10:9092")
        client.get_offset(topic='unittest1', partition=0)
        client.get_offset(topic='unittest1', partition=1)
        client.get_offset(topic='unittest1', partition=2)
        client.get_offset(topic='unittest1', partition=3)

#
#     def test_get_metadata(self):
#         client = kafka.client.KafkaClient("192.168.33.10:9092")
#         client.conns[0].send = lambda x, y: True
#         # use data previously captured from the wire
#         client.conns[0].recv = lambda a: base64.b64decode("AAAAAAAAAAIAAAACAA0xOTIuMTY4LjQ0LjExAAAjhgAAAAEADTE5Mi4xNjguNDQuMTEAACOFAAAAAQAAAAd0b3BpYzAxAAAABAAAAAAAAAAAAAIAAAABAAAAAgAAAAEAAAACAAAAAAABAAAAAQAAAAEAAAABAAAAAQAAAAEAAAAAAAIAAAACAAAAAQAAAAIAAAABAAAAAgAAAAAAAwAAAAEAAAABAAAAAQAAAAEAAAAB")
#         client.get_metadata()
#         self.assertEqual(client.topics_to_brokers[kafka.protocol.TopicAndPartition(topic='topic01', partition=1)], kafka.protocol.BrokerMetadata(nodeId=1, host='192.168.33.10', port=9093))
#
#     def test_fetch(self):
#         client = kafka.client.KafkaClient("192.168.33.10:9092")
#         client.conns[0].send = lambda x, y: True
#         client.conns[0].recv = lambda a: base64.b64decode("AAAAAAAAAAIAAAACAA0xOTIuMTY4LjQ0LjExAAAjhgAAAAEADTE5Mi4xNjguNDQuMTEAACOFAAAAAQAAAAd0b3BpYzAxAAAABAAAAAAAAAAAAAIAAAABAAAAAgAAAAEAAAACAAAAAAABAAAAAQAAAAEAAAABAAAAAQAAAAEAAAAAAAIAAAACAAAAAQAAAAIAAAABAAAAAgAAAAAAAwAAAAEAAAABAAAAAQAAAAEAAAAB")
#         client.get_metadata()
#
#         client.conns[0].send = lambda x, y: True
#         client.conns[0].recv = lambda a: base64.b64decode("AAAAAgAAAAEAB3RvcGljMDEAAAABAAAAAQAAAAAAAAAAAAEAAAAdAAAAAAAAAAAAAAAR+osbTAAA/////wAAAANmb28=")
#         messages = client.fetch('topic01', 1, 0)
#         self.assertEqual(messages[0].message.value, "foo")
#



if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()

