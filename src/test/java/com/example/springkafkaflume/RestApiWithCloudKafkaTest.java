package com.example.springkafkaflume;

import com.example.springkafkaflume.configuration.CloudKafkaTestConfig;
import com.example.springkafkaflume.configuration.KafkaProperty;
import lombok.extern.slf4j.Slf4j;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@Slf4j
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@Import(CloudKafkaTestConfig.class)
public class RestApiWithCloudKafkaTest {

    @Autowired TestRestTemplate restTemplate;
    @Autowired KafkaProperty kafkaProperty;
    @Autowired ProducerService producerService;

    @Autowired ConsumerFactory<Long, byte[]> consumerFactory;
    private Consumer<Long, byte[]> consumer;

    @Before
    public void setUp() throws Exception {
        consumer = consumerFactory.createConsumer();
        consumer.subscribe(Collections.singleton(kafkaProperty.getTopic()));
    }

    @After
    public void tearDown() throws Exception {
        consumer.close();
    }

    @Test
    public void produce() throws IOException {

        long now = Instant.now().toEpochMilli();

        ActivityEvent activityEvent = ActivityEvent.builder()
                .activityName("LOGIN")
                .username("Hulk")
                .activityTime(now)
                .build();

        HttpEntity<ActivityEvent> request = new HttpEntity<>(activityEvent);

        ResponseEntity<String> responseEntity = restTemplate
                .postForEntity("/produce", request, String.class);

        assertThat(responseEntity.getStatusCode()).isEqualTo(HttpStatus.OK);
        assertThat(responseEntity.getBody()).isNotBlank();

        consumer = consumerFactory.createConsumer();
        consumer.subscribe(Collections.singleton(kafkaProperty.getTopic()));

        ConsumerRecord<Long, byte[]> record = KafkaTestUtils.getSingleRecord(
                consumer, kafkaProperty.getTopic(), 30_000);

        assertThat(record).isNotNull();

        byte[] bytes = record.value();
        AvroFlumeEvent flumeEvent = producerService.unpackFlumeMessage(bytes);

        Map<String, String> headers = new HashMap<>();
        flumeEvent.getHeaders().entrySet().stream().forEach(e -> {
            headers.put(e.getKey().toString(), e.getValue().toString());
        });

        assertThat(Long.parseLong(headers.get("timestamp"))).isEqualTo(now );

        ByteBuffer byteBuffer = flumeEvent.getBody();
        byte[] bytesBody = new byte[byteBuffer.remaining()];
        byteBuffer.get(bytesBody, 0, bytesBody.length);

        assertThat(ActivityEvent.deserializeByte(bytesBody)).isEqualTo(activityEvent);

    }


}
