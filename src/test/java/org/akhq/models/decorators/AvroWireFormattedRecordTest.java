package org.akhq.models.decorators;

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroSchemaUtils;
import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.akhq.Breed;
import org.akhq.Cat;
import org.akhq.configs.SchemaRegistryType;
import org.akhq.models.Record;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.reflect.ReflectData;
import org.apache.avro.reflect.ReflectDatumWriter;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.ByteArrayOutputStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Slf4j
public class AvroWireFormattedRecordTest {

    private SchemaRegistryClient schemaRegistryClient;

    @Data
    @AllArgsConstructor
    private static class MyRecord {
        private int anInt;
        private String aString;
    }

    @BeforeEach
    @SneakyThrows
    public void before() {
        schemaRegistryClient = mock(SchemaRegistryClient.class);
        ReflectData reflectData = ReflectData.get();
        Schema schema = reflectData.getSchema(MyRecord.class);
        int id = 100;
        when(schemaRegistryClient.getById(id)).thenReturn(schema);
        when(schemaRegistryClient.getSchemaById(id)).thenReturn(new AvroSchema(schema, id));
        when(schemaRegistryClient.getSchemaMetadata("mySubject", 1)).thenReturn(new SchemaMetadata(id, 1, ""));

        AvroContentTypeParser avroContentTypeParser = new AvroContentTypeParser();
    }

    @Test
    public void convertValueToWireFormatNull() {
        ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>("topic", 1, 0, new byte[0], null);
        Record akhqRecord = new Record(consumerRecord, null, null);
        AvroContentTypeMetaData metaData = AvroContentTypeMetaData.of("mySubject", 1);
        AvroWireFormattedRecord underTest = new AvroWireFormattedRecord(akhqRecord, schemaRegistryClient, metaData, SchemaRegistryType.CONFLUENT.getMagicByte());
        byte[] convertedValue = underTest.getBytesValue();
        assertNull(convertedValue);
    }

    @Test
    public void convertValueToWireFormatEmptyValue() {
        ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>("topic", 1, 0, new byte[0], new byte[0]);
        Record akhqRecord = new Record(consumerRecord, null, null);
        AvroContentTypeMetaData metaData = AvroContentTypeMetaData.of("mySubject", 1);
        AvroWireFormattedRecord underTest = new AvroWireFormattedRecord(akhqRecord, schemaRegistryClient, metaData, SchemaRegistryType.CONFLUENT.getMagicByte());
        byte[] convertedValue = underTest.getBytesValue();
        assertEquals(0, convertedValue.length);
    }

    @Test
    @SneakyThrows
    public void convertValueToWireFormatWrongContentType() {
        MyRecord record = new MyRecord(42, "leet");
        byte[] avroPayload = serializeAvro(record);

        ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>("topic", 1, 0, new byte[0], avroPayload);
        consumerRecord.headers().add(new RecordHeader("contentType", "mySubject.v1".getBytes()));
        Record akhqRecord = new Record(consumerRecord, null, null);
        AvroWireFormattedRecord underTest = new AvroWireFormattedRecord(akhqRecord, schemaRegistryClient, null, SchemaRegistryType.CONFLUENT.getMagicByte());
        byte[] convertedValue = underTest.getBytesValue();

        assertEquals(convertedValue, avroPayload);
    }

    @Test
    @SneakyThrows
    public void convertValueToWireFormatWireFormat() {
        MyRecord record = new MyRecord(42, "leet");
        byte[] avroPayload = serializeAvro(record);

        ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>("topic", 1, 0, new byte[0], avroPayload);
        consumerRecord.headers().add(new RecordHeader("contentType", "application/vnd.mySubject.v1+avro".getBytes()));
        Record akhqRecord = new Record(consumerRecord, null, null);
        AvroContentTypeMetaData metaData = AvroContentTypeMetaData.of("mySubject", 1);
        AvroWireFormattedRecord underTest = new AvroWireFormattedRecord(akhqRecord, schemaRegistryClient, metaData, SchemaRegistryType.CONFLUENT.getMagicByte());
        byte[] convertedValue = underTest.getBytesValue();

        KafkaAvroDeserializer kafkaAvroDeserializer = new KafkaAvroDeserializer(schemaRegistryClient);
        GenericData.Record deserializedRecord = (GenericData.Record) kafkaAvroDeserializer.deserialize(null, convertedValue);
        assertEquals(record.getAnInt(), deserializedRecord.get(1));
        assertEquals(record.getAString(), deserializedRecord.get(0).toString());
    }

    @Test
    @SneakyThrows
    public void decoratesOtherDecoratorsCorrectly() {

        // GIVEN avro key data
        String avroCatExampleJson = "{\"id\":10,\"name\":\"Tom\",\"breed\":\"SPHYNX\"}";
        GenericRecord avroCatExample = new GenericRecordBuilder(Cat.SCHEMA$)
                .set("id", 10)
                .set("name", "Tom")
                .set("breed", Breed.SPHYNX)
                .build();

        // AND avro wire serialized value data
        MyRecord record = new MyRecord(42, "leet");
        byte[] avroPayload = serializeAvro(record);

        // AND a record instance with both key and value bytes payload
        ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>("topic", 1, 0, new byte[0], avroPayload);
        consumerRecord.headers().add(new RecordHeader("contentType", "application/vnd.mySubject.v1+avro".getBytes()));
        Record akhqRecord = new Record(consumerRecord, null, null);

        // AND base record is decorated with an avro key deserializer
        Deserializer<Object> aMockedAvroDeserializer = Mockito.mock(KafkaAvroDeserializer.class);
        Mockito.when(aMockedAvroDeserializer.deserialize(Mockito.any(), Mockito.any())).thenReturn(avroCatExample); //
        Record decoratedRecord = new AvroKeySchemaRecord(akhqRecord, aMockedAvroDeserializer);

        // AND already decorated record is decorated with a value wire format deserializer decorator
        AvroContentTypeMetaData metaData = AvroContentTypeMetaData.of("mySubject", 1);
        AvroWireFormattedRecord underTest = new AvroWireFormattedRecord(decoratedRecord, schemaRegistryClient, metaData, SchemaRegistryType.CONFLUENT.getMagicByte());

        // EXPECT getKey() call returns a String with the original json key content
        assertThat(underTest.getKey(), is(avroCatExampleJson));
    }

    @SneakyThrows
    private byte[] serializeAvro(MyRecord record) {
        Schema schema = AvroSchemaUtils.getSchema(record, true);
        DatumWriter<MyRecord> writer = new ReflectDatumWriter<>(schema);
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        Encoder encoder = EncoderFactory.get().binaryEncoder(stream, null);
        writer.write(record, encoder);
        encoder.flush();
        return stream.toByteArray();
    }
}