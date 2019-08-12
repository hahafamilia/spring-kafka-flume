package com.example.springkafkaflume;

import lombok.extern.slf4j.Slf4j;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.flume.source.avro.AvroFlumeEvent;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Service
public class ProducerService {

    @Autowired
    KafkaTemplate kafkaTemplate;

    public void send(String topic, ActivityEvent message) throws IOException {

        Optional<Long> optionalTs = Optional.ofNullable(message.getActivityTime());
        long ts = optionalTs.orElse(Instant.now().toEpochMilli());

        // Flume message header
        Map<CharSequence, CharSequence> headers = new HashMap<>();
        headers.put("timestamp", Long.toString(ts));

        // Avro message bytes
        byte[] bytes = ActivityEvent.serializeByte(message);

        // Attatch flume message header with avro message.
        bytes = packFlumeMessage(headers, bytes);

        // 카프카 레코드 작성
        ProducerRecord<Long, byte[]> record = new ProducerRecord<>(topic, ts, bytes);

        // 카프카 전송
        ListenableFuture<SendResult> future = kafkaTemplate.send(record);

        future.addCallback(new ListenableFutureCallback<SendResult>() {
            @Override
            public void onFailure(Throwable e) {
                log.warn("failed after send to kafka: exception={}, record={}", e.getMessage(), message, e);
            }

            @Override
            public void onSuccess(SendResult sendResult) {
                log.info("success after send to kafka: record={}", message);
            }
        });


    }

    /**
     * Kafka 전송 메시지에 Flume 의 헤더 값을 추가합니다.
     * eventTime 을 Flume 에 추가하기 위함입니다.
     * Flume 은 수집시간이 아닌 이벤트 발생시간 기준으로 파티션되어 저장합니다.
     * @param headers "timestamp" 밀리초, String
     * @param message
     * @return
     * @throws IOException
     */
    private byte[] packFlumeMessage(Map<CharSequence, CharSequence> headers, byte[] message) throws IOException {

        AvroFlumeEvent avroFlumeEvent = new AvroFlumeEvent(headers, ByteBuffer.wrap(message));
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().directBinaryEncoder(out, null);
        SpecificDatumWriter<AvroFlumeEvent> writer = new SpecificDatumWriter<>(AvroFlumeEvent.class);
        writer.write(avroFlumeEvent, encoder);
        encoder.flush();

        return out.toByteArray();
    }

    /**
     * Flume Message 에서 Header/Body 를 분리합니다.
     * @param bytes
     * @return List(header, body)
     * @throws IOException
     */
    public AvroFlumeEvent unpackFlumeMessage(byte[] bytes) throws IOException {

        ByteArrayInputStream stream = new ByteArrayInputStream(bytes);
        BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(stream, null);
        SpecificDatumReader<AvroFlumeEvent> reader = new SpecificDatumReader<>(AvroFlumeEvent.class);
        AvroFlumeEvent avroFlumeEvent = reader.read(null, decoder);

        return avroFlumeEvent;

    }

}
