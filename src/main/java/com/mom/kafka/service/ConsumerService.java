package com.mom.kafka.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mom.kafka.entity.Order;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.PartitionOffset;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
public class ConsumerService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConsumerService.class);

    @Value(value = "${kafka.order.topic}")
    private String orderTopic;
    private KafkaConsumer<String, String> kafkaConsumerTransactional;

    @Autowired
    public ConsumerService(KafkaConsumer<String, String> kafkaConsumerTransactional) {
        this.kafkaConsumerTransactional = kafkaConsumerTransactional;
    }

    //Task 1
//    @KafkaListener(topics = "${kafka.order.topic}", groupId = "${kafka.consumer.group.id}")
//    public void firstListener(String jsonMsg) {
//        LOGGER.info("First Listener received {}", jsonMsg);
//        Order order;
//        try {
//            ObjectMapper objectMapper = new ObjectMapper();
//            order = objectMapper.readValue(jsonMsg, Order.class);
//            LOGGER.info("First Listener converted Order: {}", order);
//        } catch (Exception e) {
//            LOGGER.error(e.getMessage());
//        }
//    }
    //@KafkaListener(topics = "topic1, topic2", groupId = "foo")//We can implement multiple listeners for a topic, each with a different group Id. Furthermore, one consumer can listen for messages from various topics:

    @KafkaListener(
            topicPartitions = @TopicPartition(topic = "${kafka.order.topic}",
                    partitionOffsets = {@PartitionOffset(partition = "0", initialOffset = "0")}),//we can specify several partitions here
            containerFactory = "partitionsKafkaListenerContainerFactory")
    public void firstListener(@Payload String jsonMsg, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        LOGGER.info("First Listener received {}", jsonMsg);
        Order order;
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            order = objectMapper.readValue(jsonMsg, Order.class);
            LOGGER.info("First Listener converted Order: {}", order);
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        }
    }

    @KafkaListener(
            topicPartitions = @TopicPartition(topic = "${kafka.order.topic}",
                    partitionOffsets = {
                            @PartitionOffset(partition = "1", initialOffset = "0")}),//initialOffset- read all previously consumed messages
            containerFactory = "partitionsKafkaListenerContainerFactory")
    public void secondListener(@Payload String jsonMsg, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
        LOGGER.info("Second Listener received {}", jsonMsg);
        Order order;
        try {
            ObjectMapper objectMapper = new ObjectMapper();
            order = objectMapper.readValue(jsonMsg, Order.class);
            LOGGER.info("Second Listener converted Order: {}", order);
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        }
    }

    //task2
//        @KafkaListener(topics = "${kafka.order.topic}", groupId = "${kafka.consumer.group.id}")
//    public void transactionListener( @Payload final String payload) {
//            LOGGER.info("Transaction Listener received {}", payload);
//    }


}
