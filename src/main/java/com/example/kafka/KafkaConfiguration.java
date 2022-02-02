package com.example.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

public interface KafkaConfiguration {

    KafkaProducer<String,String> setProducerConfig(String localHost);
    KafkaConsumer<String,String> setConsumerConfig(String localHost, String consumerGroupId);

}
