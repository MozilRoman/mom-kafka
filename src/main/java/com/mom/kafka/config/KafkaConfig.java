package com.mom.kafka.config;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.KafkaAdmin;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Value(value = "${kafka.server}")
    private String kafkaServer;
    @Value(value = "${kafka.order.topic}")
    private String orderTopicName;

//    @Bean
//    public KafkaAdmin kafkaAdmin() {
//        Map<String, Object> configs = new HashMap<>();
//        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
//        return new KafkaAdmin(configs);
//    }
//
//    @Bean
//    public NewTopic orderTopic() {//creates topics programmatically together with KafkaAdmin
//        return new NewTopic(orderTopicName, 2, (short) 1);
//    }

}