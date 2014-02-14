# -*- coding: UTF-8 -*-
# (c)2014 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/kafka-python-basic


import struct
import logging
import itertools
import socket
import base64

import kafka.protocol

logger = logging.getLogger(__name__)

DEFAULT_SOCKET_TIMEOUT_SECONDS = 5


class KafkaConnection(object):

    def __init__(self, host, port, timeout=DEFAULT_SOCKET_TIMEOUT_SECONDS):
        self.host = host
        self.port = int(port)
        self.timeout = float(DEFAULT_SOCKET_TIMEOUT_SECONDS)
        self.sock = None

    def __str__(self):
        return "('%s', %s)" % (self.host, self.port)

    def __repr__(self):
        return "KafkaConnection('%s', %i, %f)" % (self.host, self.port, self.timeout)

    def __lt__(self, other):
        if not isinstance(other, self.__class__):
            raise TypeError('can comapre only objects of same class')
        if type(other) is tuple:
            if self.host < other[0]: return True
            if self.port < other[1]: return True
        if self.host < other.host: return True
        if self.port < other.port: return True
        return False

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            raise TypeError('can comapre only objects of same class')
        if (other.host == self.host) and (other.port == self.port):
            return True
        return False

    def connect(self):
        self.close()
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect((self.host, self.port))
        return

    def close(self):
        if self.sock: self.sock.close()
        self.sock = None
        return

    def send(self, request_id, payload):
        if not self.sock: self.connect()
        self.sock.sendall(payload)
        return

    def _read_bytes(self, size_b):
        resp = ''
        data = ''
        while size_b:
            try:
                data = self.sock.recv(size_b)
            except socket.error as exc:
                logger.warning('%r (%i)', exc, exc.errno)
                raise
            size_b -= len(data)
            resp += data
        return resp


    def recv(self, request_id):
        if not self.sock: self.connect()
        # read header
#         resp = self.sock.recv(4, socket.MSG_WAITALL)
        resp = self._read_bytes(4)
        (size,) = struct.unpack('>i', resp)
        # read the rest of the message
#         resp = self.sock.recv(size, socket.MSG_WAITALL)
        resp = self._read_bytes(size)
        data = str(resp)
        return data


