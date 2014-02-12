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
        c1 = kafka.client.KafkaClient("192.168.44.11:9093")
        c1.get_metadata()
        print(c1.__dict__)

        c = kafka.client.KafkaClient("192.168.44.11:9093")
        c.conns[0].send = lambda a, b: True
        c.conns[0].recv = lambda a: base64.b64decode("AAAAAAAAAAIAAAACAA0xOTIuMTY4LjQ0LjExAAAjhgAAAAEADTE5Mi4xNjguNDQuMTEAACOFAAAAAQAAAAd0b3BpYzAxAAAABAAAAAAAAAAAAAIAAAACAAAAAgAAAAEAAAABAAAAAgAAAAAAAQAAAAIAAAACAAAAAQAAAAIAAAABAAAAAgAAAAAAAgAAAAIAAAACAAAAAgAAAAEAAAABAAAAAgAAAAAAAwAAAAIAAAACAAAAAQAAAAIAAAABAAAAAg==")
        c.get_metadata()
        print(c.__dict__)

        self.assertEqual(c.topics_to_brokers, c1.topics_to_brokers)


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()

