package com.spring.kafka.configs;

import com.spring.configs.ApplicationProperties;
import com.spring.kafka.models.Message;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonSerializer;
import java.util.HashMap;
import java.util.Map;


@Configuration
public class KafkaConfig {
    @Autowired
    ApplicationProperties props;

    private static final String SECURITYPROTOCOL = "security.protocol";
    private static final String SASLMECHANISM = "sasl.mechanism";
    private static final String JASCONFIG = "sasl.jaas.config";
    private static final String SECURITYUSERPASS = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";";

    public String getPassKafka() {
        //Para usar a senha com crypto descomentar a linha a 31 e excluir a linha 32
//        return new Decryptor().decrypt(props.getSenhaKafka(), System.getenv(props.getPasswordDecrypt()), System.getenv(props.getAlgorithmDecrypt()));
        return props.getSenhaKafka();
    }

    @Bean
    public Map<String, Object> producerConfigs() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.props.getBootstrapAddress());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, this.props.getGroupId());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        if (this.props.getProtocol() != null) {
            String jaasTemplate = SECURITYUSERPASS;
            String jaasCfg = String.format(jaasTemplate, this.props.getSecurityUser(), getPassKafka());
            props.put(SECURITYPROTOCOL, this.props.getProtocol());
            props.put(SASLMECHANISM, this.props.getMechanism());
            props.put(JASCONFIG, jaasCfg);
        }
        return props;
    }

    @Bean
    public ProducerFactory<String, Message> producerFactory() {
        return new DefaultKafkaProducerFactory<>(producerConfigs());
    }

    @Bean
    public KafkaTemplate<String, Message> kafkaTemplateJson() {
        return new KafkaTemplate<>(producerFactory());
    }

    @Bean
    public Map<String, Object> producerConfigsString() {
        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, this.props.getBootstrapAddress());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        if (this.props.getProtocol() != null) {
            String jaasTemplate = SECURITYUSERPASS;
            String jaasCfg = String.format(jaasTemplate, this.props.getSecurityUser(), getPassKafka());
            props.put(SECURITYPROTOCOL, this.props.getProtocol());
            props.put(SASLMECHANISM, this.props.getMechanism());
            props.put(JASCONFIG, jaasCfg);
        }
        return props;
    }

    @Bean
    public ProducerFactory<String, String> producerFactoryString() {
        return new DefaultKafkaProducerFactory<>(producerConfigsString());
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplateString() {
        return new KafkaTemplate<>(producerFactoryString());
    }
}
