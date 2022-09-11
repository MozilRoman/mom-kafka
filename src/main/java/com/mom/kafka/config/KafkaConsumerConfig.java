package com.mom.kafka.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

@Configuration
public class KafkaConsumerConfig {

    @Value(value = "${kafka.server}")
    private String kafkaServer;
    @Value(value = "${kafka.consumer.group.id}")
    private String consumerGroupId;
    @Value(value = "${kafka.order.topic}")
    private String orderTopicName;

    @Bean
    public ConsumerFactory<String, String> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                kafkaServer);
        props.put(
                ConsumerConfig.GROUP_ID_CONFIG,
                consumerGroupId);
        props.put(
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);
        props.put(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");//For task 2
        return new DefaultKafkaConsumerFactory<>(props);
    }

    //for task 1
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> partitionsKafkaListenerContainerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "partitions");//other group id
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, "20971520");
        props.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, "20971520");
        DefaultKafkaConsumerFactory<Object, Object> objectObjectDefaultKafkaConsumerFactory = new DefaultKafkaConsumerFactory<>(props);

        ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(objectObjectDefaultKafkaConsumerFactory);
        return factory;
    }

    @Bean
    public KafkaConsumer<String, String> kafkaConsumerTransactional(){
        var consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroupId);
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProperties.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        consumerProperties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        var consumer = new KafkaConsumer<>(
                consumerProperties, Serdes.String().deserializer(), Serdes.String().deserializer());
        consumer.subscribe(Arrays.asList(orderTopicName));
        return consumer;
    }

}
