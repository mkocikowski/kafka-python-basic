Purpose
-------
Provide simple, single-threaded consumer and producer, compatible with
Kafka 0.8. These are to be used either as stand alone programs, or as
libraries. The producer will also expose a REST interface. 

Installation - client only
--------------------------

    # if you want the client only, no VM with zookeeper/kafka
    pip install -U https://github.com/mkocikowski/kafka-python-basic/archive/master.zip

    # if you want the VM, zookeeper/kafka, for devs:
    git clone https://github.com/mkocikowski/kafka-python-basic.git
    cd kafka-python-basic
    pip install -Ue ./
    vagrant up # make sure you have 192.168.44.11 available
    python kafka/test/units.py 

Consumer CLI
------------

    kafka-consumer --hosts 192.168.44.11:9093,192.168.44.11:9094 --topic topic01

If you have multiple brokers, you must list them all in the hosts
string (at least the brokers for the topic). There is no
'autodiscovery', so if you don't list a broker, the consumer will not
connect to it. 

If you specify a 'group', then queue offsets will be recorded in
"~/.kafka-consumer.offsets", and every time you start a consumer with
a given group, it will resume at the last offset. Note that at present
there are no provisions for multiple simultaneous consumers for a
single topic. 

Producer CLI
------------
    
    TODO

Producer REST
-------------

    TODO
