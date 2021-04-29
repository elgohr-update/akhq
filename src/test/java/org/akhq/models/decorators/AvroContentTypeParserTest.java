package org.akhq.models.decorators;

import lombok.SneakyThrows;
import org.akhq.configs.SchemaRegistryType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

public class AvroContentTypeParserTest {

    private final AvroContentTypeParser underTest = new AvroContentTypeParser();

    @Test
    public void parseContentTypeNullValue() {
        ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>("topic", 1, 0, new byte[0], null);
        Optional<AvroContentTypeMetaData> avroMeta = underTest.parseAvroContentTypeMetaData(consumerRecord, SchemaRegistryType.CONFLUENT);
        assertThat(avroMeta.isEmpty(), is(true));
    }

    @Test
    public void parseContentTypeEmptyValue() {
        ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>("topic", 1, 0, new byte[0], new byte[0]);
        Optional<AvroContentTypeMetaData> avroMeta = underTest.parseAvroContentTypeMetaData(consumerRecord, SchemaRegistryType.CONFLUENT);
        assertThat(avroMeta.isEmpty(), is(true));
    }

    @Test
    @SneakyThrows
    public void convertValueToWireFormatWrongContentType() {
        byte[] value = "anything".getBytes(StandardCharsets.UTF_8);
        ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>("topic", 1, 0, new byte[0], value);
        consumerRecord.headers().add(new RecordHeader("contentType", "mySubject.v1".getBytes()));
        Optional<AvroContentTypeMetaData> avroMeta = underTest.parseAvroContentTypeMetaData(consumerRecord, SchemaRegistryType.CONFLUENT);
        assertThat(avroMeta.isEmpty(), is(true));
    }

    @Test
    @SneakyThrows
    public void convertValueToWireFormatWireFormat() {
        byte[] value = "anything".getBytes(StandardCharsets.UTF_8);
        ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>("topic", 1, 0, new byte[0], value);
        consumerRecord.headers().add(new RecordHeader("contentType", "application/vnd.mySubject.v1+avro".getBytes()));
        Optional<AvroContentTypeMetaData> avroMeta = underTest.parseAvroContentTypeMetaData(consumerRecord, SchemaRegistryType.CONFLUENT);
        assertThat(avroMeta.isPresent(), is(true));
        assertThat(avroMeta.get().getSubject(), is("mySubject"));
        assertThat(avroMeta.get().getVersion(), is(1));
    }
}