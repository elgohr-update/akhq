package org.akhq.modules;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.akhq.KafkaTestCluster;
import org.akhq.models.decorators.AvroValueSchemaRecord;
import org.akhq.models.Record;
import org.akhq.repositories.AvroWireFormatConverter;
import org.akhq.repositories.CustomDeserializerRepository;
import org.akhq.repositories.SchemaRegistryRepository;
import org.akhq.utils.ProtobufToJsonDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class RecordFactoryTest {

    @Test
    public void testNoKeyStringValue() {
        KafkaModule kafkaModule = mock(KafkaModule.class);
        ProtobufToJsonDeserializer protoBufDeserializer = mock(ProtobufToJsonDeserializer.class);
        when(protoBufDeserializer.deserialize(anyString(), any(), anyBoolean())).thenReturn(null);
        CustomDeserializerRepository customDeserializerRepository = mock(CustomDeserializerRepository.class);
        when(customDeserializerRepository.getProtobufToJsonDeserializer(any())).thenReturn(protoBufDeserializer);
        AvroWireFormatConverter avroWireFormatConverter = mock(AvroWireFormatConverter.class);
        SchemaRegistryRepository schemaRegistryRepository = mock(SchemaRegistryRepository.class);
        when(schemaRegistryRepository.determineAvroSchemaForPayload(any(), any())).thenReturn(null);
        RecordFactory underTest = new RecordFactory(kafkaModule, customDeserializerRepository, avroWireFormatConverter, schemaRegistryRepository);

        byte[] key = null;
        byte[] value = "anyValue".getBytes(StandardCharsets.UTF_8);
        ConsumerRecord<byte[], byte[]> kafkaRecord = new ConsumerRecord<>("egal", 0, 0, key, value);

        Record akhqRecord = underTest.newRecord(kafkaRecord, KafkaTestCluster.CLUSTER_ID);
        assertThat(akhqRecord, instanceOf(Record.class));
        assertThat(akhqRecord.getKey(), is(nullValue()));
        assertThat(akhqRecord.getValue(), is("anyValue"));
    }

    @Test
    public void testNoKeyAvroValue() {
        KafkaModule kafkaModule = mock(KafkaModule.class);
        CustomDeserializerRepository customDeserializerRepository = mock(CustomDeserializerRepository.class);
        AvroWireFormatConverter avroWireFormatConverter = mock(AvroWireFormatConverter.class);
        SchemaRegistryRepository schemaRegistryRepository = mock(SchemaRegistryRepository.class);
        when(schemaRegistryRepository.getKafkaAvroDeserializer(anyString())).thenReturn(new KafkaAvroDeserializer());
        when(schemaRegistryRepository.determineAvroSchemaForPayload(any(), any())).thenReturn(null).thenReturn(1);
        RecordFactory underTest = new RecordFactory(kafkaModule, customDeserializerRepository, avroWireFormatConverter, schemaRegistryRepository);

        String jsonValue = "{\"id\":10,\"name\":\"Tiger\",\"weight\":\"10.40\"}";
        byte[] key = null;
        byte[] value = jsonValue.getBytes(StandardCharsets.UTF_8);
        byte[] magic = new byte[1 + value.length];

        System.arraycopy(new byte[] {0x0}, 0, magic, 0, 1);
        System.arraycopy(value, 0, magic, 1, value.length);
        ConsumerRecord<byte[], byte[]> kafkaRecord = new ConsumerRecord<>("egal", 0, 0, key, magic);

        Record akhqRecord = underTest.newRecord(kafkaRecord, "none");
        assertThat(akhqRecord, instanceOf(AvroValueSchemaRecord.class));
    }
}