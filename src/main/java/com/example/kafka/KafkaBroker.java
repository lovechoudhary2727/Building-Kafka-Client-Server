package com.example.kafka;

public interface KafkaBroker {

    void sendMessage(String msg);
    void receiveMessage(PrintReadMessage printReadMessage);

}
