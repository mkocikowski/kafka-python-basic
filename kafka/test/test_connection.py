# -*- coding: UTF-8 -*-
# (c)2013 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/esbench

import unittest
import logging

import kafka.client


class ConnectionTest(unittest.TestCase):

    def test_init(self):
        c = kafka.client.KafkaConnection("192.168.44.11", 9093)
        self.assertEqual(c, ("192.168.44.11", 9093))
        self.assertEqual(c, kafka.client.KafkaConnection("192.168.44.11", 9093))
        self.assertNotEqual(c, kafka.client.KafkaConnection("192.168.44.11", 9094))

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()

