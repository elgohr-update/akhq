package org.akhq.modules;

import org.akhq.configs.Connection;
import org.akhq.configs.SchemaRegistryType;
import org.akhq.models.Record;
import org.akhq.repositories.AvroWireFormatConverter;
import org.akhq.repositories.CustomDeserializerRepository;
import org.akhq.repositories.RecordRepository;
import org.akhq.repositories.SchemaRegistryRepository;
import org.akhq.utils.ProtobufToJsonDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.nio.ByteBuffer;

@Singleton
public class RecordFactory {

    @Inject
    private KafkaModule kafkaModule;

    @Inject
    private CustomDeserializerRepository customDeserializerRepository;

    @Inject
    private AvroWireFormatConverter avroWireFormatConverter;

    @Inject
    private SchemaRegistryRepository schemaRegistryRepository;

    private Deserializer kafkaAvroDeserializer;
    private ProtobufToJsonDeserializer protobufToJsonDeserializer;

    public Record newRecord(ConsumerRecord<byte[], byte[]> record, String clusterId) {
        final SchemaRegistryType schemaRegistryType = this.schemaRegistryRepository.getSchemaRegistryType(clusterId);

        return new Record(
                record,
                determineAvroSchema(schemaRegistryType, record.key()),
                determineAvroSchema(schemaRegistryType, record.value()),
                this.schemaRegistryRepository.getKafkaAvroDeserializer(clusterId),
                this.customDeserializerRepository.getProtobufToJsonDeserializer(clusterId),
                avroWireFormatConverter.convertValueToWireFormat(record, this.kafkaModule.getRegistryClient(clusterId),
                        this.schemaRegistryRepository.getSchemaRegistryType(clusterId))
        );
    }

    public Record newRecord(ConsumerRecord<byte[], byte[]> record, RecordRepository.BaseOptions options) {
        final SchemaRegistryType schemaRegistryType = this.schemaRegistryRepository.getSchemaRegistryType(options.getClusterId());

        return new Record(
                record,
                determineAvroSchema(schemaRegistryType, record.key()),
                determineAvroSchema(schemaRegistryType, record.value()),
                this.schemaRegistryRepository.getKafkaAvroDeserializer(options.getClusterId()),
                this.customDeserializerRepository.getProtobufToJsonDeserializer(options.getClusterId()),
                avroWireFormatConverter.convertValueToWireFormat(record, this.kafkaModule.getRegistryClient(options.getClusterId()),
                        this.schemaRegistryRepository.getSchemaRegistryType(options.getClusterId()))
        );
    }

    private Integer determineAvroSchema(SchemaRegistryType schemaRegistryType, byte[] payload) {

        try {
            final ByteBuffer buffer = ByteBuffer.wrap(payload);
            final byte magicBytes = buffer.get();
            final int schemaId = buffer.getInt();

            if (magicBytes == schemaRegistryType.getMagicByte() && schemaId >= 0) {
                return schemaId;
            }
        } catch (Exception ignore) {

        }
        return null;
    }
}