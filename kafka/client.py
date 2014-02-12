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
        return self.__str__()

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

    def recv(self, request_id):
        if not self.sock: self.connect()
        # read header
        resp = self.sock.recv(4, socket.MSG_WAITALL)
        (size,) = struct.unpack('>i', resp)
        # read the rest of the message
        resp = self.sock.recv(size, socket.MSG_WAITALL)
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

        return


    def send_request(self, request_id, request):

        for conn in self.conns:
            try:
                conn.send(request_id, request)
                response = conn.recv(request_id)
                return response
            except Exception as exc:
                logger.debug(exc)
                continue

        return False


    def get_metadata(self):

        self.brokers = {}            # broker_id -> BrokerMetadata
        self.topics_to_brokers = {}  # topic_id -> broker_id
        self.topic_partitions = {}   # topic_id -> [0, 1, 2, ...]

        request_id = KafkaClient.ID_GEN.next()
        request = kafka.protocol.encode_metadata_request(self.client_id, request_id, topics=None)
        logger.debug("metadata request: %s", base64.b64encode(request))
        response = self.send_request(request_id, request)
        logger.debug("metadata response: %s", base64.b64encode(response))
        self.brokers, topics = kafka.protocol.decode_metadata_response(response)
        for topic, partitions in topics.items():
            if not partitions: continue
            self.topic_partitions[topic] = []
            for partition, meta in partitions.items():
                topic_part = kafka.protocol.TopicAndPartition(topic, partition)
                self.topics_to_brokers[topic_part] = self.brokers[meta.leader]
                self.topic_partitions[topic].append(partition)

        return
