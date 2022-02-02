package com.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class KafkaBrokerImp implements KafkaBroker {

    KafkaConfiguration kafkaConfiguration;

    KafkaProducer<String,String> producer;
    KafkaConsumer<String,String> consumer;

    public KafkaBrokerImp(String localHost,String consumerGroupId){

        this.producer = kafkaConfiguration.setProducerConfig(localHost);
        this.consumer = kafkaConfiguration.setConsumerConfig(localHost,consumerGroupId);

    }

    @Override
    public void sendMessage(String msg) {
        //final Logger logger = LoggerFactory.getLogger(Producer.class);

        for(int i=50;i<=60;i++){
            // Creating a Producer record
            ProducerRecord<String,String> record = new ProducerRecord<>("sampleTopic2","key"+i,msg+i);

            //Sending record - Asynchronous
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e == null){
                        System.out.println("Record written");
                        //record successfully written
                        //logger.info("Record written Successfully\n"+
                        //"topic: "+ recordMetadata.topic()+" & " + "partition: " + recordMetadata.partition()
                        //+ " & offset: " + recordMetadata.offset()+"\n");
                    }
                    else{
                        //logger.error("Error Found",e);
                        System.out.println("Error Occured");
                        System.out.println(e);
                    }

                }
            });
        }

        producer.flush();   //It writes the pending producer records to kafka, if any
        producer.close();   //Now producer can't able to send anymore records to kafka topic

    }

    @Override
    public void receiveMessage(PrintReadMessage printReadMessage) {

        //Subscribing to kafka topics
        consumer.subscribe(Arrays.asList("sampleTopic2"));

        //pulling and consuming records
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(5);

        scheduler.scheduleAtFixedRate(()->{

            //poll will be used to read the data from topic and its return type is record
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for(ConsumerRecord record:records){
                printReadMessage.print(record);
            }
        },0,2, TimeUnit.SECONDS);


        scheduler.shutdown();

    }
}
