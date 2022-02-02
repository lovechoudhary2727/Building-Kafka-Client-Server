package com.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class KafkaClient {
    public static void main(String[] args) throws InterruptedException {

        KafkaBroker broker = new KafkaBrokerImp("localhost:9092","java-group-consumer");
        broker.sendMessage("Hello");
        broker.receiveMessage(new PrintReadMessage() {
            @Override
            public void print(ConsumerRecord record) {
                System.out.println("\nRecord Received:\n" +
                        "Key: "+ record.key()+" & " + "value: " + record.value()
                        + " & topic: "+record.topic()+ " & partition: " + record.partition()+
                        " & offset: " + record.offset()+"\n");
            }
        });

    }
}
