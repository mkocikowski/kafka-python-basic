#!/usr/bin/env bash

apt-get update

apt-get install -y vim
apt-get install -y curl
apt-get install -y git
apt-get install -y build-essential
apt-get install -y openjdk-7-jre-headless

apt-get install -y python-dev # needed to compile pycrypto for ansible
apt-get install -y python-pip
apt-get install -y python-virtualenv
apt-get install -y ipython

# this is a modification of the default kafka install
# the changes are in the ./config directory, including the
# server-1.properties and server-2.properties files, and the
# supervisord-programs.conf file; 
tar -zxvf /vagrant/Vbootstrap.tgz

# install supervisord and all zookeeper and kafka to it (supervisord-programs.conf)
apt-get install -y supervisor
sudo ln -sf /home/vagrant/kafka_2.8.0-0.8.0/config/supervisord-programs.conf /etc/supervisor/conf.d/
# sudo kill -SIGHUP `cat /var/run/supervisord.pid`
sudo pkill -SIGHUP supervisord$
sleep 5

# create the base topics
/home/vagrant/kafka_2.8.0-0.8.0/bin/kafka-create-topic.sh --zookeeper 192.168.44.11:2181 --partition 4 --replica 2  --topic unittest01
/home/vagrant/kafka_2.8.0-0.8.0/bin/kafka-create-topic.sh --zookeeper 192.168.44.11:2181 --partition 4 --replica 2  --topic topic01

# pre-populate with some data
echo "foo" | /home/vagrant/kafka_2.8.0-0.8.0/bin/kafka-console-producer.sh --broker-list 192.168.44.11:9093,192.168.44.11:9094 --topic unittest01 --sync
echo "bar" | /home/vagrant/kafka_2.8.0-0.8.0/bin/kafka-console-producer.sh --broker-list 192.168.44.11:9093,192.168.44.11:9094 --topic unittest01 --sync
