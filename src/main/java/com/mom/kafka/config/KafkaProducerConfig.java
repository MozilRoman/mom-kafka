package com.mom.kafka.config;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Configuration
public class KafkaProducerConfig {

    @Value(value = "${kafka.server}")
    private String kafkaServer;
    @Value(value = "${spring.kafka.producer.properties.transactional.id}")
    private String transactionalId;
    @Value(value = "${spring.kafka.producer.properties.enable.idempotence}")
    private String transactionEnableIdempotence;

    @Bean
    public ProducerFactory<String, String> producerFactory() {
        Map<String, Object> configProps = new HashMap<>();
        configProps.put(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafkaServer);
        configProps.put(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class);
        configProps.put(
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class);
//        configProps.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);//Task 2
        return new DefaultKafkaProducerFactory<>(configProps);
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public KafkaProducer<String, String> kafkaProducerTransactional() {
        var producerProperties = new Properties();
        producerProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        producerProperties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, transactionEnableIdempotence);
        producerProperties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        producerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        return new KafkaProducer<>(producerProperties);
    }

}
