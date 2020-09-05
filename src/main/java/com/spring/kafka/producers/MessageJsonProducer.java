package com.spring.kafka.producers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.spring.kafka.models.Message;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;
import org.springframework.util.concurrent.ListenableFuture;


@Component
public class MessageJsonProducer {
    private static Logger logger = LoggerFactory.getLogger(MessageJsonProducer.class);

    @Autowired
    private KafkaTemplate<String, Message> kafkaTemplate;

    public ListenableFuture<SendResult<String, Message>> sendMessage(String topic, Message message) {
        logger.info(String.format("Acessando tópico %s e enviando mensagem %s ", topic, message.getData()));
        return kafkaTemplate.send(topic, message);
    }
}