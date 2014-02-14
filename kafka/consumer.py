import logging
import time
import argparse

import kafka.log
import kafka.client
import kafka.protocol


logger = logging.getLogger(__name__)


class KafkaConsumer(object):

    def __init__(self, client, group, topic, failfast=False):
        self.failfast = failfast
        self.client = client
        self.group = group
        self.topic = topic
        self.offsets = {}
        self.offsets_pending = {}


    def init_offsets(self):

        if not self.client.topic_partitions:
            self.client.get_metadata()

        try:
            self.offsets = {p: 0 for p in self.client.topic_partitions[self.topic]}
#             logger.debug(self.offsets)

        except KeyError:
            raise kafka.protocol.UnknownTopicError(self.topic)

        return


    def load_offsets(self):
        pass


    def save_offsets(self):
        pass


    def commit(self):

        if not self.offsets_pending:
            return

        self.offsets = self.offsets_pending
        self.offsets_pending = {}

        return


    def rollback(self):

        self.offsets_pending = {}
        return


    def fetch(self):

        if not self.offsets:
            self.init_offsets()
            self.load_offsets()

        self.commit()

        self.offsets_pending = self.offsets.copy()
        values = []

        for partition in self.offsets:

            try:
                messages = self.client.fetch(self.topic, partition, self.offsets[partition])
                offset = max(messages, key=lambda x: x.offset) if messages else None
                if offset: self.offsets_pending[partition] = offset.offset + 1
                for m in messages:
                    values.append(m.message.value)

            except (IOError, kafka.protocol.BrokerResponseError) as exc:
                logger.warning("fetching topic: %s, partition: %i; %r" % (self.topic, partition, exc))
                if self.failfast:
                    raise

        return values



def args_parser():

    parser = argparse.ArgumentParser(description="Kafka consumer cli (%s)" % (kafka.__version__, ), epilog="")
    parser.add_argument('--hosts', type=str, action='store', default='localhost:9092', help="broker1:port1,broker2:port2; (%(default)s)")
    parser.add_argument('--topic', type=str, action='store', default=None, help="topic name; (%(default)s)")
    parser.add_argument('--failfast', action='store_true')

    return parser


def main():

    args = args_parser().parse_args()

    client = kafka.client.KafkaClient(args.hosts)
    consumer = KafkaConsumer(client, 'g1', args.topic, failfast=args.failfast)

    while True:
        messages = consumer.fetch()
        print(messages)
        time.sleep(1)


if __name__ == "__main__":

    kafka.log.set_up_logging(level=logging.INFO)
    main()


