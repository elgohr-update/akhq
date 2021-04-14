package org.akhq.models.decorators;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.akhq.Breed;
import org.akhq.Cat;
import org.akhq.models.Record;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class AvroKeySchemaRecordTest {

    @Test
    @Tag("UnitTest")
    public void testGetKeyAvroSerialized() {

        // Test data preparation
        String avroCatExampleJson = "{\"id\":10,\"name\":\"Tom\",\"breed\":\"SPHYNX\"}";
        GenericRecord avroCatExample = aCatExample(10, "Tom", Breed.SPHYNX);

        // GIVEN a record with avro serialized key bytes
        byte[] keyBytes = avroCatExampleJson.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = "".getBytes(StandardCharsets.UTF_8);
        ConsumerRecord<byte[], byte[]> kafkaRecord = new ConsumerRecord<>("topic", 0, 0, keyBytes, valueBytes);
        Record record = new Record(kafkaRecord, 1, 2);

        // WHEN this record is decorated
        Deserializer<Object> aMockedAvroDeserializer = Mockito.mock(KafkaAvroDeserializer.class);
        Mockito.when(aMockedAvroDeserializer.deserialize(Mockito.any(), Mockito.any())).thenReturn(avroCatExample); //
        Record decoratedRecord = new AvroKeySchemaRecord(record, aMockedAvroDeserializer);

        // EXPECT getKey() call returns a String with original json content
        assertThat(decoratedRecord.getKey(), is(avroCatExampleJson));
    }

    @Test
    @Tag("UnitTest")
    public void testGetKeyAvroSerializedFallback() {

        // Test data
        String avroCatExampleJson = "{\"id\":10,\"name\":\"Tom\",\"breed\":\"SPHYNX\"}";

        // GIVEN a record with avro serialized key bytes
        byte[] keyBytes = avroCatExampleJson.getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = null; // value does not matter for this test
        ConsumerRecord<byte[], byte[]> kafkaRecord = new ConsumerRecord<>("topic", 0, 0, keyBytes, valueBytes);
        Record record = new Record(kafkaRecord, 1, 2);

        // AND decorated with an avro deserializer decorator
        Deserializer<Object> aMockedAvroDeserializer = Mockito.mock(KafkaAvroDeserializer.class);
        record = new AvroKeySchemaRecord(record, aMockedAvroDeserializer);

        // WHEN avro deserializer throws an exception
        Mockito.when(aMockedAvroDeserializer.deserialize(Mockito.any(), Mockito.any()))
                .then(invocation -> { throw new NullPointerException("Exception"); });

        // EXPECT getKey() returns a string representation of the value bytes
        assertThat(record.getKey(), is(avroCatExampleJson));
    }

    /**
     * Method returns an avro example data object with a cat schema
     */
    private GenericRecord aCatExample(int id, String name, Breed breed) {
        return new GenericRecordBuilder(Cat.SCHEMA$)
                .set("id", id)
                .set("name", name)
                .set("breed", breed)
                .build();
    }
}
