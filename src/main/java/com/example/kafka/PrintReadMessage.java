package com.example.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public interface PrintReadMessage {

    void print(ConsumerRecord record);

}
