#!/bin/bash

/home/kafka/current/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 4 --topic topic1 &> /dev/null
/home/kafka/current/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 4 --topic unittest1 &> /dev/null

for MESSAGE in "foo" "bar"
do
  echo $MESSAGE | /home/kafka/current/bin/kafka-console-producer.sh --broker-list 192.168.33.10:9092 --topic unittest1 --sync &> /dev/null
done
