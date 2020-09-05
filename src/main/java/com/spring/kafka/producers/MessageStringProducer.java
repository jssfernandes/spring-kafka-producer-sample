package com.spring.kafka.producers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;


@Component
public class MessageStringProducer {
    private static Logger logger = LoggerFactory.getLogger(MessageStringProducer.class);

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public ListenableFuture<SendResult<String, String>> sendMessage(String topic, String message) {
        logger.info(String.format("Acessando tópico %s e enviando mensagem %s ", topic, message));
        return kafkaTemplate.send(topic, message);
    }
}