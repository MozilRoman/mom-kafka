package com.mom.kafka.service;

import com.mom.kafka.entity.Order;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.Arrays;
import java.util.regex.Pattern;

@EnableKafkaStreams
@Configuration
public class StreamService {

    private final Logger LOGGER = LoggerFactory.getLogger(StreamService.class);
    private static String STREAM_TOPIC_1 = "stream-topic-1";
    private static String STREAM_TOPIC_2 = "stream-topic-2";
    private static String STREAM_TOPIC_3 = "stream-topic-3";

    @Bean
    public KStream<String, String> kStreamConsumeFromTopic1(StreamsBuilder builder) {    //word counter
        Pattern pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS);
        KStream<String, String> kStreamIn = builder.stream(STREAM_TOPIC_1, Consumed.with(Serdes.String(), Serdes.String()));

        kStreamIn.foreach((k, v) -> LOGGER.info("kStreamConsumeFromTopic1: Incoming msg has key: " + k + ", and val: " + v));

        KTable<String, Long> wordCounts = kStreamIn.flatMapValues(value -> Arrays.asList(pattern.split(value.toLowerCase()))).groupBy((key, word) -> word).count();

        wordCounts.toStream().foreach((w, c) -> LOGGER.info("word: " + w + " -> " + c));

        kStreamIn.mapValues((key, value) -> value.toUpperCase()).peek((key, value) -> LOGGER.info("To uppercase Val:  VALUE \"{}\" with key \"{} \"  was processed", value, key)).to(STREAM_TOPIC_2, Produced.with(Serdes.String(), new JsonSerde<>()));

        return kStreamIn;
    }

    @Bean
    public KStream<String, String> kStreamConsumeFromTopic2(StreamsBuilder builder) {
        KStream<String, String> kStreamIn = builder.stream(STREAM_TOPIC_2, Consumed.with(Serdes.String(), Serdes.String()));

        kStreamIn.foreach((k, v) -> LOGGER.info("kStreamConsumeFromTopic2: Incoming msg has key: " + k + ", and val: " + v));

        return kStreamIn;
    }

    @Bean
    public KStream<String, Order> kStreamConsumeFromTopic3(StreamsBuilder builder) {
        KStream<String, Order> kStreamIn = builder.stream(STREAM_TOPIC_3, Consumed.with(Serdes.String(), getOrderDeserializer()));

        kStreamIn.foreach((k, v) -> LOGGER.info("kStreamConsumeFromTopic3: Incoming msg has key: " + k + ", and val: " + v));

        return kStreamIn;
    }
    
    //@Bean
    //public KStream<String, String> kStreamConsumeFromTwoTopics(StreamsBuilder builder) {
    //    var topic4Stream = builder.stream(topic4, Consumed.with(Serdes.String(), Serdes.String()));
    //    var topic5Stream = builder.stream(topic5, Consumed.with(Serdes.String(), Serdes.String()));
    //    var merged = topic4Stream.merge(topic5Stream);
//
//        merged.foreach((k, v) -> log.info("Merged received key [{}] with value [{}]. ", k, v));
//
//        var processed = merged
//                .filter((k, v) -> Objects.nonNull(v) && v.contains(":"))
//                .map((KeyValueMapper<String, String, KeyValue<String, String>>) (k, v) -> new KeyValue<>(v.split(":")[0]
//                        , v.split(":")[1]));
//
//        processed.foreach((k, v) -> log.info("Words count [{}] - [{}]", k, v));
//
//        return merged;
//    }



    public static Serde<Order> getOrderDeserializer() {
        JsonSerializer<Order> serializer = new JsonSerializer<>();
        JsonDeserializer<Order> deserializer = new JsonDeserializer<>(Order.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }
}
