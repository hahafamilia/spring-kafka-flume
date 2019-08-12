package com.example.springkafkaflume;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroFactory;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import com.fasterxml.jackson.dataformat.avro.schema.AvroSchemaGenerator;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.util.Utf8;
import org.apache.commons.lang.ObjectUtils;

import java.io.IOException;

@Slf4j
@Data
@Builder
public class ActivityEvent {
    String username;
    String activityName;
    Long activityTime;

    private static final AvroSchema SCHEMA;

    static{

        AvroSchema schema = null;

        try {

            ObjectMapper mapper = new ObjectMapper(new AvroFactory());
            AvroSchemaGenerator gen = new AvroSchemaGenerator();
            mapper.acceptJsonFormatVisitor(ActivityEvent.class, gen);

            schema = gen.getGeneratedSchema();

            log.info(schema.getAvroSchema().toString(true));

        } catch (IOException e) {
            e.printStackTrace();
        }

        SCHEMA = schema;

    }

    public static byte[] serializeByte(ActivityEvent data) throws JsonProcessingException {
        AvroMapper mapper = new AvroMapper();
        return mapper.writer(SCHEMA).writeValueAsBytes(data);
    }

    public static ActivityEvent deserializeByte(byte[] bytes) throws IOException {
        DatumReader<GenericRecord> datumReader;
        datumReader = new GenericDatumReader<GenericRecord>(SCHEMA.getAvroSchema());
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bytes, null);
        GenericRecord record = datumReader.read(null, decoder);

        Utf8 userIp = (Utf8)record.get("activityName");
        Utf8 path = (Utf8)record.get("username");
        Integer status = (Integer)record.get("status");
        Long timestamp = (Long)record.get("activityTime");

        return ActivityEvent.builder()
                .activityName(ObjectUtils.toString(userIp, null))
                .username(ObjectUtils.toString(path, null))
                .activityTime(timestamp)
                .build();
    }

}
