# -*- coding: UTF-8 -*-
# (c)2014 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/kafka-python-basic

import logging
import time
import argparse
import os.path
import json

import kafka.log
import kafka.client
import kafka.protocol


logger = logging.getLogger(__name__)


OFFSETS_FILE_PATH = "~/.kafka-consumer.offsets"

class KafkaConsumer(object):

    def __init__(self, client, group, topic, failfast=False):
        self.failfast = failfast
        self.client = client
        self.group = group
        self.topic = topic
        self.offsets = {}
        self.offsets_pending = {}


    def __enter__(self):
        return self
    
    
    def __exit__(self, exctype, value, tb): 
        self.save_offsets()
        

    def init_offsets(self):

        if not self.client.topic_partitions:
            self.client.get_metadata()

        try:
            self.offsets = {p: 0 for p in self.client.topic_partitions[self.topic]}
            logger.debug(self.offsets)

        except KeyError:
            raise kafka.protocol.UnknownTopicError(self.topic)

        return


    def load_offsets(self):

        if not self.group:
            return

        try: 
            path = os.path.expanduser(OFFSETS_FILE_PATH)
            with open(path, "rU") as f:
                offsets = f.read()
            offsets = json.loads(offsets)
            # json doesn't allow integer keys, all keys are strings,
            # so here the partition keys need to be coneverted back to ints
            self.offsets = {int(k): v for k, v in offsets[self.topic].items()}
            logger.debug("loaded offsets from file: %s, %s", path, self.offsets)

        except IOError as exc: 
            logger.warning("can't open offsets file: %s", exc)
        
        except ValueError as exc: 
            logger.warning("can't parse offsets file: %s", exc)
        
        return
        

    def save_offsets(self):
    
        if not self.group:
            return
        
        path = os.path.expanduser(OFFSETS_FILE_PATH)
        try: 
            with open(path, "rU") as f:
                offsets = f.read()
                offsets = json.loads(offsets)
        except IOError, ValueError:
            offsets = {}

        try: 
            offsets[self.topic] = self.offsets
            logger.debug(offsets)
            logger.debug(json.dumps(offsets))
            with open(path, "w") as f:
                f.write(json.dumps(offsets))
            logger.debug("saved offsets to file: %s", path)

        except IOError as exc: 
            logger.warning("can't open offsets file: %s", exc)
        
        return
        

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

    epilog = """
There is little automagic here. You need to specify all your brokers
in the hosts parameter (there is no magic boostraping of the config).
If you set the 'failfast' flag, then the program will exit on first
connection failure. Otherwise, when there is a problem with connection
to individual broker, the connection will be retried. The basic idea
is to run this in failfast mode under supervisord or something like
that.
"""

    parser = argparse.ArgumentParser(description="Kafka consumer cli (%s)" % (kafka.__version__, ), epilog=epilog)
    parser.add_argument('--hosts', type=str, action='store', default='localhost:9092', help="broker1:port1,broker2:port2; (%(default)s)")
    parser.add_argument('--topic', type=str, action='store', default=None, help="topic name; (%(default)s)")
    parser.add_argument('--group', type=str, action='store', default=None, help="if set, use this group id; (%(default)s)")
    parser.add_argument('--failfast', action='store_true', help="if set, exit on any error")
    parser.add_argument('--verbose', '-v', action='count', default=0, help="try -v, -vv, -vvv")

    return parser


def main():

    args = args_parser().parse_args()
    kafka.log.set_up_logging(level=logging.ERROR-(args.verbose*10))

    client = kafka.client.KafkaClient(args.hosts)

    with KafkaConsumer(client, args.group, args.topic, failfast=args.failfast) as consumer:
    
        while True:
            messages = consumer.fetch()
            print(messages)
            time.sleep(1)


if __name__ == "__main__":
    main()


