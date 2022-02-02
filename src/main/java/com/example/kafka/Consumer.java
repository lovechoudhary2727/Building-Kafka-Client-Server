package com.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

public class Consumer {

    public static void main(String[] args) {

        //Creating properties for Consumer Object
        Properties p = new Properties();

        p.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        p.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"java-group-consumer");
        //When a consumer is reading data for the very first time form the kafka topic, then from which offset
        // it should start its reading from, for that we will use-auto-reset-offset property.
        p.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        //Creating kafka Consumer
        final KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(p);

        //Subscribing to kafka topics
        consumer.subscribe(Arrays.asList("sampleTopic2"));

        //pulling and consuming records
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(5);

        for(int i=0;i<5;i++){
            scheduler.scheduleAtFixedRate(()->{

                //poll will be used to read the data from topic and its return type is record
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for(ConsumerRecord record:records){
                    System.out.println("\nRecord Received:\n" +
                            "Key: "+ record.key()+" & " + "value: " + record.value()
                            + " & topic: "+record.topic()+ " & partition: " + record.partition()+
                            " & offset: " + record.offset()+"\n");
                }
            },0,2,TimeUnit.SECONDS);
        }

        scheduler.shutdown();

    }
}
