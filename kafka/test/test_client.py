# -*- coding: UTF-8 -*-
# (c)2013 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/esbench

import unittest
import logging
import base64

import kafka.client


class ClientTest(unittest.TestCase):

    def test_init(self):
        client = kafka.client.KafkaClient("192.168.44.11:9093,192.168.44.11:9094")
        self.assertEqual(sorted(client.conns), [('192.168.44.11', 9093), ('192.168.44.11', 9094)])


    def test_get_metadata(self):
        client = kafka.client.KafkaClient("192.168.44.11:9093,192.168.44.11:9094")
        client.conns[0].send = lambda x, y: True
        # use data previously captured from the wire
        client.conns[0].recv = lambda a: base64.b64decode("AAAAAAAAAAIAAAACAA0xOTIuMTY4LjQ0LjExAAAjhgAAAAEADTE5Mi4xNjguNDQuMTEAACOFAAAAAQAAAAd0b3BpYzAxAAAABAAAAAAAAAAAAAIAAAACAAAAAgAAAAEAAAABAAAAAgAAAAAAAQAAAAIAAAACAAAAAQAAAAIAAAABAAAAAgAAAAAAAgAAAAIAAAACAAAAAgAAAAEAAAABAAAAAgAAAAAAAwAAAAIAAAACAAAAAQAAAAIAAAABAAAAAg==")
        client.get_metadata()
        self.assertEqual(client.topics_to_brokers[kafka.protocol.TopicAndPartition(topic='topic01', partition=3)], kafka.protocol.BrokerMetadata(nodeId=2, host='192.168.44.11', port=9094))


    def test_fetch(self):
        client = kafka.client.KafkaClient("192.168.44.11:9093,192.168.44.11:9094")
        messages = client.fetch('topic01', 0, 0)



# class IntergationClientTest(unittest.TestCase):
#
#     def test_get_metadata(self):
#         client = kafka.client.KafkaClient("192.168.44.11:9093,192.168.44.11:9094")
#         client.get_metadata()
#
#     def test_fetch(self):
#         client = kafka.client.KafkaClient("192.168.44.11:9093,192.168.44.11:9094")
#         client.fetch('topic01', 0, 0)
#         client.fetch('topic01', 1, 0)
#         client.fetch('topic01', 2, 0)
#         client.fetch('topic01', 3, 0)
#

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()

