package com.example.springkafkaflume;

import com.example.springkafkaflume.configuration.EmbeddedKafkaTestConfig;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singleton;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * 1. SpringBootTest classess 지정할 경우, 내부 Bean 만 로드 합니다.
 * 1.1 EmbeddedKafkaTestConfig ProduceService 의 내부에서 지정된 Bean 아니기에 Import 합니다.
 * 2. SpringBootTest classes 지정하지 않을 경우 모든 Bean 로드 됩니다.
 * 2.1 KafkaConfig, EmbeddedKafkaTestConfig 의 Bean이 로드 되고 KafkaConfig 의 Beasn 을 사용합니다.
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes =  ProducerService.class)
@Import(EmbeddedKafkaTestConfig.class)
@EmbeddedKafka
public class EmbeddedKafkaTest {

    private static String TOPIC = "test-topic";

    @Autowired private EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired ConsumerFactory<Long, byte[]> consumerFactory;
    private Consumer<Long, byte[]> consumer;

    @Autowired
    ProducerService producerService;

    @Before
    public void setUp() throws Exception {
        consumer = consumerFactory.createConsumer();
        consumer.subscribe(singleton(TOPIC));
    }

    @After
    public void tearDown() throws Exception {
        consumer.close();
    }

    @Test
    public void receive() throws IOException {
        long timestamp = Instant.now().toEpochMilli();

        ActivityEvent activityEvent = ActivityEvent.builder()
                .activityName("LOGIN")
                .username("Hulk")
                .activityTime(timestamp)
                .build();

        producerService.send(TOPIC, activityEvent);

        ConsumerRecord<Long, byte[]> record = KafkaTestUtils.getSingleRecord(consumer, TOPIC, 20_000);

        assertNotNull(record);

        byte[] bytes = record.value();
        AvroFlumeEvent flumeEvent = producerService.unpackFlumeMessage(bytes);

        Map<String, String> headers = new HashMap<>();
        flumeEvent.getHeaders().entrySet().stream().forEach(e -> {
            headers.put(e.getKey().toString(), e.getValue().toString());
        });

        assertEquals("activityEvent", timestamp, Long.parseLong(headers.get("timestamp")));

        ByteBuffer byteBuffer = flumeEvent.getBody();
        byte[] bytesBody = new byte[byteBuffer.remaining()];
        byteBuffer.get(bytesBody, 0, bytesBody.length);

        assertEquals("activityEvent", activityEvent, ActivityEvent.deserializeByte(bytesBody));
    }
}