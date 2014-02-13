import logging
import time

import kafka.client
import kafka.protocol


logger = logging.getLogger(__name__)

FETCH_BUFFER_SIZE_BYTES = 4096
MAX_FETCH_BUFFER_SIZE_BYTES = FETCH_BUFFER_SIZE_BYTES * 8

class Consumer(object):

    def __init__(self, client, group, topic):
        self.client = client
        self.group = group
        self.topic = topic
        try:
            self.offsets = {p: 0 for p in self.client.topic_partitions[topic]}
#             logger.debug(self.offsets)
        except KeyError:
            raise kafka.protocol.UnknownTopicError(topic)

    def fetch(self):

        requests = []
        values = []
        for partition in self.offsets:
            messages = self.client.fetch(self.topic, partition, self.offsets[partition])
            offset = max(messages, key=lambda x: x.offset) if messages else None
            if offset: self.offsets[partition] = offset.offset + 1
            for m in messages:
                values.append(m.message.value)

        return values



if __name__ == "__main__":

    logging.basicConfig(level=logging.DEBUG)

    client = kafka.client.KafkaClient("192.168.44.11:9093,192.168.44.11:9094")
    consumer = Consumer(client, 'g1', 'topic01')

    while True:
        messages = consumer.fetch()
        print(messages)
        time.sleep(1)
