import struct
import logging
import itertools
import socket
import base64

import kafka.connection
import kafka.protocol

logger = logging.getLogger(__name__)

FETCH_BUFFER_SIZE_BYTES = 2**24 # 16MB max message size, anything bigger will effectively choke the partition
ID_GEN = itertools.count()


class KafkaClient(object):

    def __init__(self, brokers):

        self.client_id = 'kafka-python'

        self.conns = []
        for broker in brokers.split(","):
            host, port = broker.split(":")
            self.conns.append(kafka.connection.KafkaConnection(host, port))

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

            except IOError as exc:
                logger.warning("conn: %s, broker: %s, %s", conn, broker, exc, exc_info=False)
                if exc.errno == 32:
                    conn.connect() # this will raise #61 if the broker went away
                continue

        raise kafka.protocol.BrokerResponseError("no responses from broker: %s" % (broker, ))


    def get_metadata(self):

        self.brokers.clear() # as opposed to self.brokers = {} this will keep the same dict instance
        self.topics_to_brokers.clear()
        self.topic_partitions.clear()

        try:
            request_id = kafka.client.ID_GEN.next()
            request = kafka.protocol.encode_metadata_request(self.client_id, request_id, topics=None)
#             logger.debug(base64.b64encode(request)) # get the wire dump
            response = self.send_request(request_id, request)
#             logger.debug(base64.b64encode(response)) # get the wire dump

            self.brokers, topics = kafka.protocol.decode_metadata_response(response)
            for topic, partitions in topics.items():
                if not partitions: continue
                self.topic_partitions[topic] = []
                for partition, meta in partitions.items():
                    topic_part = kafka.protocol.TopicAndPartition(topic, partition)
                    self.topics_to_brokers[topic_part] = self.brokers[meta.leader] if meta.leader != -1 else None
                    self.topic_partitions[topic].append(partition)

        except kafka.protocol.BrokerResponseError as exc:
            logger.debug("%r in get_metadata()", exc)

        return


    def fetch(self, topic, partition, offset):

        if not self.brokers:
            self.get_metadata()

        request_id = kafka.client.ID_GEN.next()
        request = kafka.protocol.FetchRequest(topic, partition, offset, FETCH_BUFFER_SIZE_BYTES)
        encoded = kafka.protocol.encode_fetch_request(self.client_id, request_id, request)
#         logger.debug(base64.b64encode(encoded)) # get the wire dump
        leader = self.topics_to_brokers[kafka.protocol.TopicAndPartition(topic, partition)]
#         logger.info((leader, self.topics_to_brokers))
        response = self.send_request(request_id, encoded, broker=leader)
#         logger.debug(base64.b64encode(response)) # get the wire dump

        messages = []
        for r in kafka.protocol.decode_fetch_response(response):
            for m in r.messages:
                messages.append(m)

        return messages

