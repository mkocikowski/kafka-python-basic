#  Copyright 2013 David Arthur
# 
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
# 
#        http://www.apache.org/licenses/LICENSE-2.0
# 
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

import collections
import struct
import logging
import base64
import zlib


logger = logging.getLogger(__name__)


PRODUCE_KEY = 0
FETCH_KEY = 1
OFFSET_KEY = 2
METADATA_KEY = 3
OFFSET_COMMIT_KEY = 8
OFFSET_FETCH_KEY = 9

ATTRIBUTE_CODEC_MASK = 0x03
CODEC_NONE = 0x00
CODEC_GZIP = 0x01
CODEC_SNAPPY = 0x02


ProduceRequest = collections.namedtuple("ProduceRequest", ["topic", "partition", "messages"])
FetchRequest = collections.namedtuple("FetchRequest", ["topic", "partition", "offset", "max_bytes"])
OffsetRequest = collections.namedtuple("OffsetRequest", ["topic", "partition", "time", "max_offsets"])
OffsetCommitRequest = collections.namedtuple("OffsetCommitRequest",["topic", "partition", "offset", "metadata"])
OffsetFetchRequest = collections.namedtuple("OffsetFetchRequest", ["topic", "partition"])

ProduceResponse = collections.namedtuple("ProduceResponse", ["topic", "partition", "error", "offset"])
FetchResponse = collections.namedtuple("FetchResponse", ["topic", "partition", "error", "highwaterMark", "messages"])
OffsetResponse = collections.namedtuple("OffsetResponse", ["topic", "partition", "error", "offsets"])
OffsetCommitResponse = collections.namedtuple("OffsetCommitResponse", ["topic", "partition", "error"])
OffsetFetchResponse = collections.namedtuple("OffsetFetchResponse", ["topic", "partition", "offset", "metadata", "error"])
BrokerMetadata = collections.namedtuple("BrokerMetadata", ["nodeId", "host", "port"])
PartitionMetadata = collections.namedtuple("PartitionMetadata", ["topic", "partition", "leader", "replicas", "isr"])

OffsetAndMessage = collections.namedtuple("OffsetAndMessage", ["offset", "message"])
Message = collections.namedtuple("Message", ["magic", "attributes", "key", "value"])
TopicAndPartition = collections.namedtuple("TopicAndPartition", ["topic", "partition"])


class KafkaError(RuntimeError): pass
class KafkaRequestError(KafkaError): pass
class KafkaUnavailableError(KafkaError): pass
class BrokerResponseError(KafkaError): pass
class PartitionUnavailableError(KafkaError): pass
class FailedPayloadsError(KafkaError): pass
class ConnectionError(KafkaError): pass
class BufferUnderflowError(KafkaError): pass
class ChecksumError(KafkaError): pass
class ConsumerFetchSizeTooSmall(KafkaError): pass
class ConsumerNoMoreData(KafkaError): pass

class UnknownTopicError(KafkaError): pass
class CompressionNotSupportedError(KafkaError): pass


def write_int_string(s):
    if s is None:
        return struct.pack('>i', -1)
    else:
        return struct.pack('>i%ds' % len(s), len(s), s)


def write_short_string(s):
    if s is None:
        return struct.pack('>h', -1)
    else:
        return struct.pack('>h%ds' % len(s), len(s), s)


def read_short_string(data, cur):
    if len(data) < cur + 2:
        raise BufferUnderflowError("Not enough data left")

    (strlen,) = struct.unpack('>h', data[cur:cur + 2])
    if strlen == -1:
        return None, cur + 2

    cur += 2
    if len(data) < cur + strlen:
        raise BufferUnderflowError("Not enough data left")

    out = data[cur:cur + strlen]
    return out, cur + strlen


def read_int_string(data, cur):
    if len(data) < cur + 4:
        raise BufferUnderflowError(
            "Not enough data left to read string len (%d < %d)" %
            (len(data), cur + 4))

    (strlen,) = struct.unpack('>i', data[cur:cur + 4])
    if strlen == -1:
        return None, cur + 4

    cur += 4
    if len(data) < cur + strlen:
        raise BufferUnderflowError("Not enough data left")

    out = data[cur:cur + strlen]
    return out, cur + strlen


def relative_unpack(fmt, data, cur):
    size = struct.calcsize(fmt)
    if len(data) < cur + size:
        raise BufferUnderflowError("Not enough data left")

    out = struct.unpack(fmt, data[cur:cur + size])
    return out, cur + size


def group_by_topic_and_partition(tuples):
    out = defaultdict(dict)
    for t in tuples:
        out[t.topic][t.partition] = t
    return out




def encode_message_header(client_id, correlation_id, request_key):

    return struct.pack(
        '>hhih%ds' % len(client_id),
        request_key,          # ApiKey
        0,                    # ApiVersion
        correlation_id,       # CorrelationId
        len(client_id),
        client_id,            # ClientId
    )


def encode_metadata_request(client_id, correlation_id, topics=None):

    topics = [] if topics is None else topics
    message = encode_message_header(client_id, correlation_id, METADATA_KEY)
    message += struct.pack('>i', len(topics))
    for topic in topics:
        message += struct.pack('>h%ds' % len(topic), len(topic), topic)

    data = write_int_string(message)
    return data


def decode_metadata_response(data):

    ((correlation_id, numbrokers), cur) = relative_unpack('>ii', data, 0)

    brokers = {}
    for i in range(numbrokers):
        ((nodeId, ), cur) = relative_unpack('>i', data, cur)
        (host, cur) = read_short_string(data, cur)
        ((port,), cur) = relative_unpack('>i', data, cur)
        brokers[nodeId] = BrokerMetadata(nodeId, host, port)

    ((num_topics,), cur) = relative_unpack('>i', data, cur)
    topic_metadata = {}

    for i in range(num_topics):
        ((topic_error,), cur) = relative_unpack('>h', data, cur)
        (topic_name, cur) = read_short_string(data, cur)
        ((num_partitions,), cur) = relative_unpack('>i', data, cur)
        partition_metadata = {}

        for j in range(num_partitions):
            ((partition_error_code, partition, leader, numReplicas), cur) = relative_unpack('>hiii', data, cur)
            (replicas, cur) = relative_unpack('>%di' % numReplicas, data, cur)
            ((num_isr,), cur) = relative_unpack('>i', data, cur)
            (isr, cur) = relative_unpack('>%di' % num_isr, data, cur)
            partition_metadata[partition] = PartitionMetadata(topic_name, partition, leader, replicas, isr)

        topic_metadata[topic_name] = partition_metadata

    return brokers, topic_metadata




def encode_produce_request(client_id, correlation_id, request, acks=1, timeout=1000):

    message = encode_message_header(client_id, correlation_id, PRODUCE_KEY)
    message += struct.pack('>hii', acks, timeout, 1)
    message += struct.pack('>h%dsi' % len(request.topic), len(request.topic), request.topic, 1)
    message_set = encode_message_set(request.messages)
    message += struct.pack('>ii%ds' % len(message_set), request.partition, len(message_set), message_set)

#     return struct.pack('>i%ds' % len(message), len(message), message)
    data = write_int_string(message)
    return data
    

def decode_produce_response(data):

    ((correlation_id, num_topics), cur) = relative_unpack('>ii', data, 0)

    for i in range(num_topics):
        ((strlen,), cur) = relative_unpack('>h', data, cur)
        topic = data[cur:cur + strlen]
        cur += strlen
        ((num_partitions,), cur) = relative_unpack('>i', data, cur)

        for i in range(num_partitions):
            ((partition, error, offset), cur) = relative_unpack('>ihq', data, cur)

            yield ProduceResponse(topic, partition, error, offset)





def encode_offset_request(client_id, correlation_id, request):

    message = encode_message_header(client_id, correlation_id, OFFSET_KEY)
    message += struct.pack('>ii', -1, 1)
    message += write_short_string(request.topic)
    message += struct.pack('>i', 1)
    message += struct.pack('>iqi', request.partition, request.time, request.max_offsets)

#     return struct.pack('>i%ds' % len(message), len(message), message)
    data = write_int_string(message)
    return data


def decode_offset_response(data):

    ((correlation_id, num_topics), cur) = relative_unpack('>ii', data, 0)

    for i in range(num_topics):
        (topic, cur) = read_short_string(data, cur)
        ((num_partitions,), cur) = relative_unpack('>i', data, cur)

        for i in range(num_partitions):
            ((partition, error, num_offsets,), cur) = relative_unpack('>ihi', data, cur)

            offsets = []
            for j in range(num_offsets):
                ((offset,), cur) = relative_unpack('>q', data, cur)
                offsets.append(offset)

            yield OffsetResponse(topic, partition, error, tuple(offsets))


def encode_fetch_request(client_id, correlation_id, request, max_wait_time=100, min_bytes=4096):

    message = encode_message_header(client_id, correlation_id, FETCH_KEY)
    message += struct.pack('>iiii', -1, max_wait_time, min_bytes, 1)
    message += write_short_string(request.topic)
    message += struct.pack('>i', 1)
    message += struct.pack('>iqi', request.partition, request.offset, request.max_bytes)

    data = write_int_string(message)
    return(data)


def decode_fetch_response(data):

    ((correlation_id, num_topics), cur) = relative_unpack('>ii', data, 0)

    for i in range(num_topics):
        (topic, cur) = read_short_string(data, cur)
        ((num_partitions,), cur) = relative_unpack('>i', data, cur)

        for i in range(num_partitions):
            ((partition, error, highwater_mark_offset), cur) = relative_unpack('>ihq', data, cur)
            (message_set, cur) = read_int_string(data, cur)
            yield FetchResponse(topic, partition, error, highwater_mark_offset, decode_message_set_iter(message_set))


def encode_message_set(messages):

    message_set = ""
    for message in messages:
        encoded_message = encode_message(message)
        message_set += struct.pack('>qi%ds' % len(encoded_message), 0, len(encoded_message), encoded_message)

    return message_set


def encode_message(message):

    if message.magic == 0:
        msg = struct.pack('>BB', message.magic, message.attributes)
        msg += write_int_string(message.key)
        msg += write_int_string(message.value)
        crc = zlib.crc32(msg)
        msg = struct.pack('>i%ds' % len(msg), crc, msg)
    else:
        raise Exception("Unexpected magic number: %d" % message.magic)

    return msg


def decode_message_set_iter(data):

    cur = 0
    read_message = False

    while cur < len(data):

        try:
            ((offset, ), cur) = relative_unpack('>q', data, cur)
            (msg, cur) = read_int_string(data, cur)
            for (offset, message) in decode_message(msg, offset):
                read_message = True
                yield OffsetAndMessage(offset, message)

        except BufferUnderflowError as exc:
            # this happens when there is a partial message - according
            # to the spec, the last message may be partial; here the
            # default behavior is to discard the unfinished message,
            # and get it on the next run, but if there is a single
            # message too large to fit, there is a problem - it will
            # never make it through
            if read_message is False: raise ConsumerFetchSizeTooSmall("try adjusting client.FETCH_BUFFER_SIZE_BYTES")
            else: raise StopIteration()


def decode_message(data, offset):

    ((crc, magic, att), cur) = relative_unpack('>iBB', data, 0)
    if crc != zlib.crc32(data[4:]):
        raise ChecksumError("message checksum failed")

    (key, cur) = read_int_string(data, cur)
    (value, cur) = read_int_string(data, cur)
    codec = att & ATTRIBUTE_CODEC_MASK

    if codec == CODEC_NONE:
        yield (offset, Message(magic, att, key, value))

    elif codec == CODEC_GZIP:
#         gz = gzip_decode(value)
#         for (offset, msg) in decode_message_set_iter(gz):
#             yield (offset, msg)
        raise CompressionNotSupportedError('gzip not supported yet')
        pass

    elif codec == CODEC_SNAPPY:
#         snp = snappy_decode(value)
#         for (offset, msg) in decode_message_set_iter(snp):
#             yield (offset, msg)
        raise CompressionNotSupportedError('snappy not supported yet')
        pass

