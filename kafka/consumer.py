# -*- coding: UTF-8 -*-
# (c)2014 Mik Kocikowski, MIT License (http://opensource.org/licenses/MIT)
# https://github.com/mkocikowski/kafka-python-basic

import logging
import time
import argparse
import os.path
import json
import sys

import kafka.log
import kafka.client
import kafka.protocol


logger = logging.getLogger(__name__)


OFFSETS_FILE_PATH = os.path.expanduser("~/.kafka-consumer.offsets")
WHENCE_SAVED = 0
WHENCE_HEAD = -2 # see offset api in https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol
WHENCE_TAIL = -1

class KafkaConsumerError(RuntimeError): pass


class KafkaConsumer(object):

    def __init__(self, hosts="", group="", topic="", failfast=False, whence=WHENCE_SAVED, offsets_file_path=""):
        self.failfast = failfast
        self.hosts = hosts
        self.client = kafka.client.KafkaClient(hosts)
        self.group = group
        self.topic = topic
        self.whence = whence
        self.offsets_file_path = offsets_file_path
        self.offsets = {} # {partition: offset}
        self.offsets_pending = {}
        logger.debug("created consumer: %r", self)


    def __enter__(self):
        return self
    
    
    def __exit__(self, exctype, value, tb): 
    	if exctype:
    		logger.error("exiting with error: %s, %s", exctype, tb)
        self.client.close()
        self.save_offsets()
        return False # http://docs.python.org/2/reference/datamodel.html#object.__exit__
        
        
    
    def __repr__(self):
        return "KafkaConsumer(hosts='%s', group='%s', topic='%s', failfast=%s, whence=%i, offsets_file_path='%s')" % \
            (self.hosts, self.group, self.topic, self.failfast, self.whence, self.offsets_file_path)


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
            with open(self.offsets_file_path, "rU") as f:
                offsets = f.read()
            offsets = json.loads(offsets)
            # json doesn't allow integer keys, all keys are strings,
            # so here the partition keys need to be coneverted back to ints
            if self.group in offsets: 
                self.offsets = {int(k): v for k, v in offsets[self.group][self.topic].items()}
                logger.debug("loaded offsets from file: %s, %s", self.offsets_file_path, self.offsets)
            else:
                logger.debug("group %s not in offsets file, skipping offset load", self.group)
                pass

        except IOError as exc: 
            logger.warning("can't open offsets file: %s", exc)
            if self.failfast:
                raise KafkaConsumerError("can't open offsets file: %s", self.offsets_file_path)
        
        except (ValueError, KeyError, TypeError) as exc: 
            logger.warning("can't parse offsets file: %s", exc)
            if self.failfast:
                raise KafkaConsumerError("can't parse offsets file: %s, fix it by hand or delete it", self.offsets_file_path)
        
        return
        

    def seek(self):

        if not self.offsets:
            self.init_offsets()
            self.load_offsets()

        if self.whence == WHENCE_SAVED:
            logger.debug("WHENCE_SAVED, not changing offsets: %s", self.offsets)
            return 
        
        elif self.whence == WHENCE_HEAD:
            self.offsets = {k: 0 for k, v in self.offsets.items()}
            logger.debug("WHENCE_HEAD, resetting offsets to 0: %s", self.offsets)
            return
        
        elif self.whence == WHENCE_TAIL: 
            for partition in self.offsets:
                offset = self.client.get_offset(self.topic, partition)
                self.offsets[partition] = offset.offsets[0]
                self.offsets_pending = {}
            logger.debug("WHENCE_TAIL, reset offsets_pending, set offsets: %s", self.offsets)
            return            


    def save_offsets(self):
    
        if not self.group:
            return
        
        try: 
            with open(self.offsets_file_path, "rU") as f:
                offsets = f.read()
                offsets = json.loads(offsets)
        except IOError, ValueError:
            logger.warning("can't open offsets file (for update before saving): %s", exc)
            offsets = {}
        except Exception as exc:
            logger.warning(exc, exc_info=True)
            offsets = {}

        try: 
            group_offsets = offsets.setdefault(self.group, {})
            group_offsets[self.topic] = self.offsets
#             logger.debug(offsets)
#             logger.debug(json.dumps(offsets))
            with open(self.offsets_file_path, "w") as f:
                f.write(json.dumps(offsets))
            logger.debug("saved offsets to file: %s", self.offsets_file_path)

        except IOError as exc: 
            logger.warning("can't open offsets file: %s", exc)
        except Exception as exc: 
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
            self.seek()

        # commit pending offsets before doing a read
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
You need to specify all your brokers in the hosts parameter (there is
no magic boostraping of the config). If you set the 'failfast' flag,
then the program will exit on first connection failure. Otherwise,
when there is a problem with connection to individual broker, the
connection will be retried.

If you set the 'group' argument, then offsets will be read from the
file specified with the 'offsets' argument. Setting 'head' or 'tail'
will override that. 
 
"""

    parser = argparse.ArgumentParser(description="Kafka consumer cli (%s)" % (kafka.__version__, ), epilog=epilog, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('hosts', type=str, action='store', default='localhost:9092', help="broker1:port1,broker2:port2; (%(default)s)")
    parser.add_argument('topic', type=str, action='store', default=None, help="topic name; (%(default)s)")
    parser.add_argument('--verbose', '-v', action='count', default=0, help="try -v, -vv, -vvv")
    parser.add_argument('--failfast', action='store_true', help="if set, exit on any error")
    parser.add_argument('--group', type=str, action='store', default=None, help="if set, use this group id; (%(default)s)")
    whence = parser.add_mutually_exclusive_group()
    whence.add_argument('--head', action='store_true', help='read from the beginning')
    whence.add_argument('--tail', action='store_true', help="read only 'new' messages")
    parser.add_argument('--offsets', metavar='PATH', type=str, action='store', default=OFFSETS_FILE_PATH, help="'%(default)s'")
    parser.add_argument('--output', metavar='PATH', type=str, action='store', default='/dev/stdout', help="output in 'append' mode; ('%(default)s')")
    

    return parser


def main():

    args = args_parser().parse_args()
    kafka.log.set_up_logging(level=logging.ERROR-(args.verbose*10))

    output_fh = None
    
    try: 

        if args.head: whence = WHENCE_HEAD
        elif args.tail: whence = WHENCE_TAIL
        else: whence = WHENCE_SAVED
        
        with KafkaConsumer(hosts=args.hosts, group=args.group, topic=args.topic, failfast=args.failfast, whence=whence, offsets_file_path=args.offsets) as consumer:

            while True:

                t1 = time.time()
                messages = consumer.fetch()
                bytesize = sum([len(m) for m in messages])
                if messages:
                    logger.debug("took %.2fs to fetch %i messages, bytesize: %i", time.time()-t1, len(messages), bytesize)

                t1 = time.time()
                for message in messages:
                    for retry in range(3): 
                        try: 
                            output_fh.write("%s\n" % message)
                            break
                        except (IOError, AttributeError) as exc: 
                            logger.debug(exc)
                            if args.output == '/dev/stdout': 
                                output_fh = sys.stdout
                            else:
                                output_fh = open(os.path.abspath(args.output), 'a', buffering=1)
                            logger.debug("opened %r (%s) for output", output_fh, args.output)


#                     output_fh.flush()
                if messages:
                    logger.debug("took %.2fs to output %i messages, bytesize: %i", time.time()-t1, len(messages), bytesize)

                # if there were no messages, sleep for a bit
                if not messages: 
                    time.sleep(1)


    except KeyboardInterrupt:
        logger.info("keyboard interrupt")
        
    finally:
        if output_fh:
            output_fh.flush()
            output_fh.close()
            logger.debug("flushed and closed output file: %r (%s)", output_fh, args.output)


if __name__ == "__main__":
    main()

