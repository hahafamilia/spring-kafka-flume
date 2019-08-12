package com.example.springkafkaflume.configuration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

import java.util.HashMap;
import java.util.Map;

@TestConfiguration
public class CloudKafkaTestConfig {

    @Autowired
    KafkaProperty kafkaProperty;

    public Map<String, Object> consumerConfig(){
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperty.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "cloudkafka-test");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");


        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        props.put(SaslConfigs.SASL_JAAS_CONFIG, kafkaProperty.getSaslJaasConfig());
        props.put(SaslConfigs.SASL_MECHANISM, kafkaProperty.getSaslMechanism());
        props.put("security.protocol", kafkaProperty.getSecurityProtocol());
        return props;
    }

    @Bean
    public ConsumerFactory<Long, byte[]> consumerFactory() {
        return new DefaultKafkaConsumerFactory<>(consumerConfig());
    }

    @Bean
    public KafkaListenerContainerFactory<ConcurrentMessageListenerContainer<Long, byte[]>> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<Long, byte[]> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }

}

