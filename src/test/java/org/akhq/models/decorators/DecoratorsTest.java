package org.akhq.models.decorators;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.akhq.models.Record;
import org.akhq.utils.ProtobufToJsonDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;

public class DecoratorsTest {

    /**
     * This test evaluates if it is possible to stack two decorators onto a base record
     * and method delegation works correctly
     */
    @Test
    @Tag("UnitTest")
    public void testAvroKeyProtobufValueDecoration() {

        /* Test data definition */
        String topic = "egal";
        Integer keySchemaId = 1;
        Integer valueSchemaId = 2;
        byte[] keyBytes = "{\"name\": \"Count\",\"namespace\": \"org.akhq\",\"type\": \"record\",\"fields\" : [{\"name\": \"count\", \"type\": \"int\"},{\"name\": \"key\", \"type\": \"string\"}]}".getBytes(StandardCharsets.UTF_8);
        byte[] valueBytes = "syntax = \"proto3\"; package org.akhq.utils; import \"google/protobuf/wrappers.proto\"; option java_package = \"org.akhq.utils\"; option java_outer_classname = \"BookProto\"; string title = 1; string author = 2; google.protobuf.DoubleValue price = 3;}".getBytes(StandardCharsets.UTF_8);

        /* Preparations (unit under test + mocks) */
        // Create a base record
        ConsumerRecord<byte[], byte[]> kafkaRecord = new ConsumerRecord<>(topic, 0, 0, keyBytes, valueBytes);
        Record record = new Record(kafkaRecord, keySchemaId, valueSchemaId);

        // 1. Decorate record with Avro deserializer for key property
        Deserializer<Object> aMockedAvroDeserializer = Mockito.mock(KafkaAvroDeserializer.class);
        record = new AvroKeySchemaRecord(record, aMockedAvroDeserializer);

        // 2. Decorate record with Protobuf deserializer for value property
        ProtobufToJsonDeserializer aMockedProtobufDeserializer = Mockito.mock(ProtobufToJsonDeserializer.class); // NOTICE: protobufToJsonDeserializer does not implement Deserializer interface
        record = new ProtoBufValueSchemaRecord(record, aMockedProtobufDeserializer);

        /* Evaluation */
        // NOTICE: We do not evaluate if deserialized record object is valid.
        // This testcase evaluates if correct deserializers are called and decoration of decorators work
        try {
            String keyString = record.getKey();
        } catch(NullPointerException e) {
            // NOTICE: Our mocked deserializer returns null, which causes a null-pointer exception.
            // We do not want to evaluate a valid result here, so we ignore this exception.
        }
        String valueString = record.getValue();

        // 1. Evaluate getKey() method calls .deserialize() from Avro deserializer
        Mockito.verify(aMockedAvroDeserializer, Mockito.times(1)).deserialize(topic, keyBytes);

        // 2. Evaluate getValue() method calls .deserialize() from ProtoBuf deserializer
        Mockito.verify(aMockedProtobufDeserializer, Mockito.times(1)).deserialize(topic, valueBytes, false);
    }
}
