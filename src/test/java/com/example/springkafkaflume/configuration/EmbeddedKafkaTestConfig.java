package com.example.springkafkaflume.configuration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.*;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import java.util.HashMap;
import java.util.Map;

@TestConfiguration
@EmbeddedKafka
public class EmbeddedKafkaTestConfig {

    @Autowired
    private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Bean
    public ProducerFactory<Long, byte[]> producerFactory() {
        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.producerProps(embeddedKafkaBroker));
        return new DefaultKafkaProducerFactory<>(configs,
                new LongSerializer(), new ByteArraySerializer());
    }

    @Bean
    public KafkaTemplate<Long, byte[]> kafkaTemplate(ProducerFactory producerFactory) {
        KafkaTemplate<Long, byte[]> kafkaTemplate = new KafkaTemplate<>(producerFactory);
        return kafkaTemplate;
    }

    @Bean
    public ConsumerFactory<Long, byte[]> consumerFactory() {

        Map<String, Object> configs = new HashMap<>(KafkaTestUtils.consumerProps(
                "embeddedkafka-test",
                "true",
                embeddedKafkaBroker));

        // offset 정보가 없습니다.
        configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new DefaultKafkaConsumerFactory<>(configs, new LongDeserializer(), new ByteArrayDeserializer());
    }


}
