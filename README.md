## Message oriented middleware(MOM): Kafka project

### Start Kafka

Install Kafka and Zookeeper locally. Run they.

Create topic using command `./kafka-topics.bat --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 2 --topic order-topic`

### Project Overview

Configs placed in application.properties

#### Task 1:

Send msg to topic. 2 consumers subscribed to topic to different partitions. Msg-s routed to partitions using Round Robbin algorithm.

#### Task 2:

Producer commit msg-s in transaction. If transaction committed- consumer can read msg-s.

For this task uncomment lined marked with `Task 2` in app.prop, KafkaProducerConfig, ProducerService

### Kafka Stream

Prerequisites:

- Create stream-topic-1 with command `./kafka-topics.bat --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic stream-topic-1`
- Create stream-topic-2 with command `./kafka-topics.bat --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic stream-topic-2`
- Create stream-topic-3 with command `./kafka-topics.bat --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic stream-topic-3`

#### Task 3:

Send any msg(row String) to `stream-topic-2`. Consumer consumes msg and logs it

To send msg use utility with command `./kafka-console-producer.bat -broker-list localhost:9092 -topic stream-topic-1`

#### Task 4:

Send any msg(row String) to `stream-topic-1`. Consumer consumes msg, count words, logs it and in the end redirect msg to `stream-topic-2`

#### Task 5:

Json Deserializer. Send json to `stream-topic-3`. Consumer consumes msg and logs it

To send msg use utility with command `./kafka-console-producer.bat -broker-list localhost:9092 -topic stream-topic-3`

Msg example `{"name":"TV", "id":123}`

