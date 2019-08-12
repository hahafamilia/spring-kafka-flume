package com.example.springkafkaflume.configuration;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import java.util.HashMap;
import java.util.Map;


@Slf4j
@Configuration
@EnableKafka
public class KafkaConfig {

    @Autowired
    KafkaProperty kafkaProperty;

    public Map<String, Object> producerConfig() {
        Map<String, Object> props = new HashMap<>();

        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperty.getBootstrapServers());

        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        props.put(SaslConfigs.SASL_JAAS_CONFIG, kafkaProperty.getSaslJaasConfig());
        props.put(SaslConfigs.SASL_MECHANISM, kafkaProperty.getSaslMechanism());
        props.put("security.protocol", kafkaProperty.getSecurityProtocol());

        return props;
    }

    public ProducerFactory<Long, byte[]> producerFactory(){
        return new DefaultKafkaProducerFactory<>(producerConfig());
    }

    @Bean
    public KafkaTemplate<Long, byte[]> kafkaTemplate(){

        return new KafkaTemplate<Long, byte[]>(producerFactory());
    }


}
