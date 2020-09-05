package com.spring.controllers;

import com.spring.kafka.models.Message;
import com.spring.kafka.producers.MessageJsonProducer;
import com.spring.models.Customer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import javax.servlet.http.HttpServletRequest;


@Controller
@RequestMapping("/producers")
public class KafkaController {
    @Value("${kafka.topic.name}")
    private String topicKafka;

    @Autowired
    private MessageJsonProducer messageJsonProducer;

    @PostMapping(produces = "application/json")
    @ResponseStatus(HttpStatus.OK)
    public ResponseEntity<Customer> testeMsg(HttpServletRequest request, @RequestBody Customer msg) {
        try {
            messageJsonProducer.sendMessage(topicKafka, new Message(msg));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return null;
    }
}
