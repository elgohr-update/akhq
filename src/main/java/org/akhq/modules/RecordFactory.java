package org.akhq.modules;

import org.akhq.configs.Connection;
import org.akhq.models.Record;
import org.akhq.repositories.AvroWireFormatConverter;
import org.akhq.repositories.CustomDeserializerRepository;
import org.akhq.repositories.RecordRepository;
import org.akhq.repositories.SchemaRegistryRepository;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import javax.inject.Inject;
import javax.inject.Singleton;

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

    public Record newRecord(ConsumerRecord<byte[], byte[]> record, String clusterId) {
        return new Record(
                record,
                this.schemaRegistryRepository.getSchemaRegistryType(clusterId),
                this.schemaRegistryRepository.getKafkaAvroDeserializer(clusterId),
                this.customDeserializerRepository.getProtobufToJsonDeserializer(clusterId),
                avroWireFormatConverter.convertValueToWireFormat(record, this.kafkaModule.getRegistryClient(clusterId),
                        this.schemaRegistryRepository.getSchemaRegistryType(clusterId))
        );
    }

    public Record newRecord(ConsumerRecord<byte[], byte[]> record, RecordRepository.BaseOptions options) {
        return new Record(
                record,
                this.schemaRegistryRepository.getSchemaRegistryType(options.getClusterId()),
                this.schemaRegistryRepository.getKafkaAvroDeserializer(options.getClusterId()),
                this.customDeserializerRepository.getProtobufToJsonDeserializer(options.getClusterId()),
                avroWireFormatConverter.convertValueToWireFormat(record, this.kafkaModule.getRegistryClient(options.getClusterId()),
                        this.schemaRegistryRepository.getSchemaRegistryType(options.getClusterId()))
        );
    }
}