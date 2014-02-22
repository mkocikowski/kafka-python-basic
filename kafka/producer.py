# -*- coding: UTF-8 -*-
# (c)2014 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/kafka-python-basic


import logging
import time
import argparse
import os.path
import json
import sys
import itertools
import select

import kafka.log
import kafka.client
import kafka.protocol


logger = logging.getLogger(__name__)

SEND_EVERY_N_BYTES = 2**20 # 1MB
SEND_EVERY_N_SECONDS = 2


class KafkaProducerError(RuntimeError): pass


class KafkaProducer(object):

    def __init__(self, hosts="", topic="", failfast=False):
        self.failfast = failfast
        self.hosts = hosts
        self.client = kafka.client.KafkaClient(hosts)
        self.topic = topic
        self.router = itertools.cycle(self.client.topic_partitions[self.topic])
        logger.debug("created producer: %r", self)


    def __enter__(self):
        return self
    
    
    def __exit__(self, exctype, value, tb): 
    	if exctype:
    		logger.error("exiting with error: %s, %s", exctype, tb)
        self.client.close()
        return False # http://docs.python.org/2/reference/datamodel.html#object.__exit__
                
    
    def __repr__(self):
        return "KafkaProducer(hosts='%s', topic='%s', failfast=%s)" % (self.hosts, self.topic, self.failfast)


    def send(self, payloads): 
        
        messages = [kafka.protocol.Message(0, 0, None, payload) for payload in payloads]
        partitioned = [[] for i in self.client.topic_partitions[self.topic]]
        for payload in payloads:
            partitioned[self.router.next()].append(kafka.protocol.Message(0, 0, None, payload))
        
        for partition, messages in enumerate(partitioned):
            response = self.client.send(self.topic, partition, messages)
            if response.error: 
                logger.error(response)
        
        return (partitioned, response) # for testing


def args_parser():

    epilog = """
You need to specify all your brokers in the hosts parameter (there is
no magic boostraping of the config). If you set the 'failfast' flag,
then the program will exit on first connection failure. Otherwise,
when there is a problem with connection to individual broker, the
connection will be retried.
 
"""

    parser = argparse.ArgumentParser(description="Kafka producer cli (%s)" % (kafka.__version__, ), epilog=epilog, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('hosts', type=str, action='store', default='localhost:9092', help="broker1:port1,broker2:port2; (%(default)s)")
    parser.add_argument('topic', type=str, action='store', default=None, help="topic name; (%(default)s)")
    parser.add_argument('--verbose', '-v', action='count', default=0, help="try -v, -vv, -vvv")
    parser.add_argument('--failfast', action='store_true', help="if set, exit on any error")
    parser.add_argument('--input', metavar='PATH', type=str, action='store', default='/dev/stdin', help="one message per line; ('%(default)s')")
    
    return parser


def main():

    args = args_parser().parse_args()
    kafka.log.set_up_logging(level=logging.ERROR-(args.verbose*10))

    try: 

        if args.input in ['/dev/stdin', '-']: 
            input_fh = sys.stdin
        else:
            input_fh = open(os.path.abspath(args.input), 'r', buffering=1)

        with KafkaProducer(hosts=args.hosts, topic=args.topic, failfast=args.failfast) as producer:

            input_exhausted = False
            while not input_exhausted:

                lines = []
                size_b = 0
                stop_at_bytes = SEND_EVERY_N_BYTES
                stop_at_time = time.time() + SEND_EVERY_N_SECONDS
        
                while (size_b < stop_at_bytes) and (time.time() < stop_at_time):

                    r, _, _ = select.select([input_fh], [], [], 1)
        
                    if not r:
                        continue
            
                    line = r[0].readline()
                    if line:
                        lines.append(line.strip("\n"))
                        size_b += len(line)
                    else:
                        input_exhausted = True
                        logger.debug("input exhausted")
                        break
        
                if lines:
                    producer.send(lines)
                    logger.debug(lines)


    except KeyboardInterrupt:
        logger.info("keyboard interrupt")

    
    finally:
        if input_fh:
            input_fh.close()
            logger.debug("closed input file: %r (%s)", input_fh, args.input)


if __name__ == "__main__":
    main()

