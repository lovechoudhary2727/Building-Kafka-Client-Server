package com.example.kafka;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class Producer {
    public static void main(String[] args) {

        final Logger logger = LoggerFactory.getLogger(Producer.class);

        //Creating a property object for Producer
        Properties prop = new Properties();

        //1. bootstrap server, prop.setProperty("bootstrap.server","127.0.0.1.9092");
        prop.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");

        //2.key-serializer, prop.setProperty("key.serializer", StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //3.value-serializer, prop.setProperty("value.serializer",StringSerializer.class.getName());
        prop.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Creating the kafka Producer
        final KafkaProducer<String,String> producer = new KafkaProducer<String, String>(prop);

        for(int i=50;i<=60;i++){
            // Creating a Producer record
            ProducerRecord<String,String> record = new ProducerRecord<>("sampleTopic2","key"+i,"value"+i);

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
}
