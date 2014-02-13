import struct
import logging
import itertools
import socket
import base64

import kafka.protocol

logger = logging.getLogger(__name__)

DEFAULT_SOCKET_TIMEOUT_SECONDS = 5
FETCH_BUFFER_SIZE_BYTES = 4096


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
        if type(other) is tuple:
            if self.host < other[0]: return True
            if self.port < other[1]: return True
        if self.host < other.host: return True
        if self.port < other.port: return True
        return False

    def __eq__(self, other):
        if (self.host, self.port) == other: return True
        if other.host != self.host: return False
        if other.port != self.port: return False
        return True

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
        while size_b:
            try:
                data = self.sock.recv(size_b)
            except socket.error as exc:
                logger.warning('unable to receive data from kafka: %s', exc)
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


class KafkaClient(object):

    ID_GEN = itertools.count()

    def __init__(self, brokers):

        self.client_id = 'kafka-python'

        self.conns = []
        for broker in brokers.split(","):
            host, port = broker.split(":")
            self.conns.append(KafkaConnection(host, port))

        self.brokers = {}            # broker_id -> BrokerMetadata
        self.topics_to_brokers = {}  # topic_id -> broker_id
        self.topic_partitions = {}   # topic_id -> [0, 1, 2, ...]

        self.get_metadata()

        return


    def close(self):
        for conn in self.conns:
            conn.close()


    def send_request(self, request_id, request, broker=None):

        for conn in self.conns:

            if broker:
                if (conn.host, conn.port) != (broker.host, broker.port):
                    continue

            try:
                conn.send(request_id, request)
                response = conn.recv(request_id)
                return response

            except Exception as exc:
                logger.error("conn: %s, broker: %s, %s", conn, broker, exc, exc_info=True)
                continue

        raise Exception("no responses from brokers")


    def get_metadata(self):

        self.brokers = {}            # broker_id -> BrokerMetadata
        self.topics_to_brokers = {}  # topic_id -> broker_id
        self.topic_partitions = {}   # topic_id -> [0, 1, 2, ...]

        request_id = KafkaClient.ID_GEN.next()
        request = kafka.protocol.encode_metadata_request(self.client_id, request_id, topics=None)
#         logger.debug(base64.b64encode(request))
        response = self.send_request(request_id, request)
#         logger.debug(base64.b64encode(response))
        self.brokers, topics = kafka.protocol.decode_metadata_response(response)
        for topic, partitions in topics.items():
            if not partitions: continue
            self.topic_partitions[topic] = []
            for partition, meta in partitions.items():
                topic_part = kafka.protocol.TopicAndPartition(topic, partition)
                self.topics_to_brokers[topic_part] = self.brokers[meta.leader]
                self.topic_partitions[topic].append(partition)

        return


    def fetch(self, topic, partition, offset):

        request_id = KafkaClient.ID_GEN.next()
        request = kafka.protocol.FetchRequest(topic, partition, offset, FETCH_BUFFER_SIZE_BYTES)
        encoded = kafka.protocol.encode_fetch_request(self.client_id, request_id, request)
#         logger.debug(base64.b64encode(encoded))
        leader = self.topics_to_brokers[kafka.protocol.TopicAndPartition(topic, partition)]
        response = self.send_request(request_id, encoded, broker=leader)
#         logger.debug(base64.b64encode(response))

        messages = []
        for r in kafka.protocol.decode_fetch_response(response):
            for m in r.messages:
                messages.append(m)

        return messages

