package com.mom.kafka.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mom.kafka.entity.Order;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.concurrent.ThreadLocalRandom;

@Service
public class ProducerService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ProducerService.class);
    @Value(value = "${kafka.order.topic}")
    private String orderTopicName;
    private KafkaTemplate<String, String> kafkaTemplate;
    private KafkaProducer<String, String> kafkaProducerTransactional;

    @Autowired
    public ProducerService(KafkaTemplate<String, String> kafkaTemplate,
                           KafkaProducer<String, String> kafkaProducerTransactional
    ) {
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaProducerTransactional = kafkaProducerTransactional;
    }

    public void send() throws JsonProcessingException {
        Order phoneOrder = new Order("Phone", ThreadLocalRandom.current().nextInt(1, 1000));
        ObjectMapper objectMapper = new ObjectMapper();
        String phoneOrderString = objectMapper.writeValueAsString(phoneOrder);

        ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(orderTopicName, phoneOrderString);

        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
            @Override
            public void onSuccess(SendResult<String, String> result) {
                LOGGER.info("Sent message=[" + phoneOrderString + "] to " + orderTopicName +
                        "topic with offset=[" + result.getRecordMetadata().offset() + "]");
            }

            @Override
            public void onFailure(Throwable ex) {
                LOGGER.error("Unable to send message=["
                        + phoneOrderString + "] to " + orderTopicName + " topic due to : " + ex.getMessage());
            }
        });
    }

    public void sendMessageWithTransaction() {
        LOGGER.info("Send in transaction.");
        kafkaProducerTransactional.initTransactions();
        try {
            kafkaProducerTransactional.beginTransaction();
            ObjectMapper objectMapper = new ObjectMapper();
            Order phoneOrder = new Order("Phone", ThreadLocalRandom.current().nextInt(1, 1000));
            String phoneOrderString = objectMapper.writeValueAsString(phoneOrder);
            Order tvOrder = new Order("TV", ThreadLocalRandom.current().nextInt(1, 1000));
            String tvOrderString = objectMapper.writeValueAsString(tvOrder);
            Order laptopOrder = new Order("Laptop", ThreadLocalRandom.current().nextInt(1, 1000));
            String laptopOrderString = objectMapper.writeValueAsString(laptopOrder);

            kafkaProducerTransactional.send(new ProducerRecord<>(orderTopicName, null, phoneOrderString));
//            if(true){//Task 2- uncomment to test transaction
//                LOGGER.info("Throwing exception");
//                throw new RuntimeException();
//            }
            kafkaProducerTransactional.send(new ProducerRecord<>(orderTopicName, null, tvOrderString));
            kafkaProducerTransactional.send(new ProducerRecord<>(orderTopicName, null, laptopOrderString));

            kafkaProducerTransactional.commitTransaction();
        } catch (KafkaException | JsonProcessingException e) {
            LOGGER.error("Abort transaction");
            kafkaProducerTransactional.abortTransaction();
        }
    }

}
