package com.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.Properties;

public class KafkaConfigurationImp implements KafkaConfiguration{

    @Override
    public KafkaProducer<String,String> setProducerConfig(String localHost) {
        //Creating a property object for Producer
        Properties prop = new Properties();

        //1. bootstrap server, prop.setProperty("bootstrap.server","127.0.0.1.9092");
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,localHost);

        //2.key-serializer, prop.setProperty("key.serializer", StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //3.value-serializer, prop.setProperty("value.serializer",StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Creating the kafka Producer
        final KafkaProducer<String,String> producer = new KafkaProducer<String, String>(prop);

        return producer;
    }

    @Override
    public KafkaConsumer<String,String> setConsumerConfig(String localHost,String consumerGroupId) {

        Properties p = new Properties();

        p.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,localHost);
        p.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.setProperty(ConsumerConfig.GROUP_ID_CONFIG,consumerGroupId);
        //When a consumer is reading data for the very first time form the kafka topic, then from which offset
        // it should start its reading from, for that we will use-auto-reset-offset property.
        p.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //Creating kafka Consumer
        final KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(p);

        return consumer;

    }
}
