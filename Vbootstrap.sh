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

pip install supervisor

# this is a modification of the default kafka install
# the changes are in the ./config directory, including the
# server-1.properties and server-2.properties files, and the
# supervisord.conf file; with this installed, you can run:
# sudo supervisord -c /home/vagrant/kafka_2.8.0-0.8.0/config/supervisord.conf
# this will give you a zookeeper node and a 2-broker kafka system running, 
# with the brokers on ports 9093 and 9094
tar -zxvf /vagrant/Vbootstrap.tgz

