kafka-storm-fcd-process
=======================
+ Use kafka and storm to process traffic information.
+ There are six machines on storm cluster.
+ Modify the six machines's hosts in /etc/hosts to node01~node06.

##build for running on a Storm cluster:
+ mvn clean package -P cluster
+ copy target/kafka-storm-fcd-process-0.0.2-jar-with-dependencies.jar to node01/node02

## zookeeper
+ zookeeper-3.4.6
+ running on node02/node03/node04
+ ```bin/zkServer.sh start```
+ ```bin/zkCli.sh```
```create /kafka '' ```

##kafka
+ kafka_2.9.2_0.8.1.1
+ running on node02
+ ```bin/kafka-server-start.sh cofig/server.properties &```
+ ```bin/kafka-topics.sh --create --zookeeper node02:2181,node03:2181,node04:2181/kafka --replication-factor 1 --partitions 1 --topic stormseven```

##storm
+ apache-storm-0.9.3
+ nimbus running on node01, supervisor running on node05/node06
+ ```bin/storm nimbus &```
+ ```bin/storm ui &```
+ ```bin/storm supervisor &```

##Running the test topologies on a storm cluster
+ on node01: ```storm jar kafka-storm-fcd-process-0.0.2-jar-with-dependencies.jar storm.kafka.KafkaSpoutTestTopology sentences node01```
+ on node02: ```java -cp kafka-storm-fcd-process-0.0.2-jar-with-dependencies.jar storm.kafka.tools.StormProducer node02:9092```
+ result show in: ```http://node01:8080```
