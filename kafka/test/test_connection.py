# -*- coding: UTF-8 -*-
# (c)2014 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/esbench

import unittest
import logging
import struct
import itertools

import kafka.connection
import kafka.protocol


logger = logging.getLogger(__name__)

class MockSocket(object):
    SEND = ""
    RECV = None # this must be an iterator: iter("foo")
    def __init__(self): pass
    def settimeout(self, timeout): pass
    def connect(self, (host, port)): pass
    def sendall(self, data): MockSocket.SEND = data
    def recv(self, size_b): return "".join(itertools.islice(MockSocket.RECV, 0, size_b))


class ConnectionTest(unittest.TestCase):

    def setUp(self):
        self.tmp_socket = kafka.connection.socket.socket
        kafka.connection.socket.socket = lambda a, y: MockSocket()

    def tearDown(self):
        kafka.connection.socket.socket = self.tmp_socket

    def test_init(self):
        c = kafka.connection.KafkaConnection("192.168.44.11", 9093)

    def test_equality(self):
        c = kafka.connection.KafkaConnection("192.168.44.11", 9093)
        self.assertEqual(c, kafka.connection.KafkaConnection("192.168.44.11", 9093))
        self.assertNotEqual(c, kafka.connection.KafkaConnection("192.168.44.11", 9094))

    def test_send(self):
        c = kafka.connection.KafkaConnection("192.168.44.11", 9093)
        c.send(1, "foo")
        self.assertEqual(MockSocket.SEND, "foo")

    def test_recv(self):
        c = kafka.connection.KafkaConnection("192.168.44.11", 9093)
        MockSocket.RECV = iter(struct.pack(">i%ds" % 3, 3, "bar"))
        data = c.recv(1)
        self.assertEqual(data, "bar")



if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()

