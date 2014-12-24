kafka-storm-fcd-process
=======================

Use kafka and storm to process traffic information.

##kafka
kafka_2.9.2_0.8.1.1

##storm
0.9.2-incubating

##build for running on a Storm cluster:
mvn clean package -P cluster

##Running the test topologies on a storm cluster
```
storm jar target/storm-kafka-0.8-plus-test-0.2.0-SNAPSHOT-jar-with-dependencies.jar storm.kafka.KafkaSpoutTestTopology sentences node01
