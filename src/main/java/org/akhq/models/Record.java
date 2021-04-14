package org.akhq.models;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.*;
import org.akhq.configs.SchemaRegistryType;
import org.akhq.utils.AvroToJsonSerializer;
import org.akhq.utils.ProtobufToJsonDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.ByteBuffer;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;

@ToString
@EqualsAndHashCode
@Getter
@NoArgsConstructor
public class Record {
    protected String topic;
    private int partition;
    private long offset;
    private ZonedDateTime timestamp;
    @JsonIgnore
    private TimestampType timestampType;
    private Integer keySchemaId;
    private Integer valueSchemaId;
    private Map<String, String> headers = new HashMap<>();
    // @Getter(AccessLevel.NONE) // NOTICE: Decorator pattern needs access to this for delegation
    protected byte[] bytesKey;
    @Getter(AccessLevel.NONE)
    protected String key;
    // @Getter(AccessLevel.NONE) // NOTICE: Decorator pattern needs access to this for delegation
    protected byte[] bytesValue;
    @Getter(AccessLevel.NONE)
    protected String value;

    protected final List<String> exceptions = new ArrayList<>();

    public Record(Record outer) {
        this.topic = outer.topic;
        this.partition = outer.partition;
        this.offset = outer.offset;
        this.timestamp = outer.timestamp;
        this.timestampType = outer.timestampType;
        this.keySchemaId = outer.keySchemaId;
        this.valueSchemaId = outer.valueSchemaId;
        this.headers = outer.headers;
        this.bytesKey = outer.bytesKey;
        this.key = outer.key;
        this.bytesValue = outer.bytesValue;
        this.value = outer.value;
    }

    public Record(RecordMetadata record, Integer keySchemaId, Integer valueSchemaId, byte[] bytesKey, byte[] bytesValue, Map<String, String> headers) {
        this.topic = record.topic();
        this.partition = record.partition();
        this.offset = record.offset();
        this.timestamp = ZonedDateTime.ofInstant(Instant.ofEpochMilli(record.timestamp()), ZoneId.systemDefault());
        this.bytesKey = bytesKey;
        this.keySchemaId = keySchemaId;
        this.bytesValue = bytesValue;
        this.valueSchemaId = valueSchemaId;
        this.headers = headers;
    }

    public Record(ConsumerRecord<byte[], byte[]> record, Integer keySchemaId, Integer valueSchemaId) {
        this.topic = record.topic();
        this.partition = record.partition();
        this.offset = record.offset();
        this.timestamp = ZonedDateTime.ofInstant(Instant.ofEpochMilli(record.timestamp()), ZoneId.systemDefault());
        this.timestampType = record.timestampType();
        this.bytesKey = record.key();
        this.keySchemaId = keySchemaId;
        this.bytesValue = record.value();
        this.valueSchemaId = valueSchemaId;
        for (Header header: record.headers()) {
            this.headers.put(header.key(), header.value() != null ? new String(header.value()) : null);
        }
    }

    public String getKey() {
        if(this.bytesKey == null) {
            return null;
        }

        if(this.key == null) {
            this.key = new String(this.bytesKey);
        }
        return this.key;
    }

    public String getValue() {
        if(this.bytesValue == null) {
            return null;
        }

        if(this.value == null) {
            this.value = new String(this.bytesValue);
        }
        return this.value;
    }
}