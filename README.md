This is prototype work. Don't expect much. 

Purpose
-------
Provide simple, single-threaded consumer and producer, compatible with
Kafka 0.8. The consumer and the producer can be used as stand alone
applications, or as python libraries. The producer exposes a REST interface. 

Installation
------------

    # if you want the client only, no VM with zookeeper/kafka
    pip install -U https://github.com/mkocikowski/kafka-python-basic/archive/master.zip

Installation with Kafka broker running on Vagrant VM
----------------------------------------------------

    git clone https://github.com/mkocikowski/kafka-python-basic.git
    cd kafka-python-basic
    pip install -Ue ./
    # you will need Ansible installed
    # make sure you have 192.168.33.10 available
    cd vagrant; vagrant up; cd .. 
    python kafka/test/units.py 

Consumer CLI
------------

	kafka-consumer --help 
    kafka-consumer 192.168.33.10:9092 topic1 --tail

Producer CLI
------------
    
    kafka-producer --help
    echo "foo" | kafka-producer 192.168.33.10:9092 topic1

Producer REST
-------------

    TODO

Credit
------
Based on [kafka-python](https://github.com/mumrah/kafka-python). The
only file largely unmodified is 'protocol.py', which has its own
license and copyright attribution. Thank you!

