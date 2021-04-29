package org.akhq.modules;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import org.akhq.configs.SchemaRegistryType;
import org.akhq.models.Record;
import org.akhq.models.decorators.*;
import org.akhq.repositories.CustomDeserializerRepository;
import org.akhq.repositories.RecordRepository;
import org.akhq.repositories.SchemaRegistryRepository;
import org.akhq.utils.ProtobufToJsonDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.Deserializer;

import javax.inject.Singleton;
import java.util.Optional;

@Singleton
public class RecordFactory {

    private final KafkaModule kafkaModule;
    private final CustomDeserializerRepository customDeserializerRepository;
    private final SchemaRegistryRepository schemaRegistryRepository;
    private final AvroContentTypeParser avroContentTypeParser;

    public RecordFactory(KafkaModule kafkaModule,
                         CustomDeserializerRepository customDeserializerRepository,
                         SchemaRegistryRepository schemaRegistryRepository,
                         AvroContentTypeParser avroContentTypeParser) {
        this.kafkaModule = kafkaModule;
        this.customDeserializerRepository = customDeserializerRepository;
        this.schemaRegistryRepository = schemaRegistryRepository;
        this.avroContentTypeParser = avroContentTypeParser;
    }

    public Record newRecord(ConsumerRecord<byte[], byte[]> record, String clusterId) {
        SchemaRegistryType schemaRegistryType = this.schemaRegistryRepository.getSchemaRegistryType(clusterId);
        SchemaRegistryClient registryClient = kafkaModule.getRegistryClient(clusterId);
        Integer keySchemaId = schemaRegistryRepository.determineAvroSchemaForPayload(schemaRegistryType, record.key());
        Integer valueSchemaId = schemaRegistryRepository.determineAvroSchemaForPayload(schemaRegistryType, record.value());

        // base record (default: string)
        Record akhqRecord = new Record(record, keySchemaId, valueSchemaId);

        // avro wire format
        Optional<AvroContentTypeMetaData> avroContentTypeMetaData = avroContentTypeParser.parseAvroContentTypeMetaData(record, schemaRegistryType);
        if(avroContentTypeMetaData.isPresent()) {
            AvroContentTypeMetaData avrometa = avroContentTypeMetaData.get();
            akhqRecord = new AvroWireFormattedRecord(akhqRecord, registryClient, avrometa, schemaRegistryType.getMagicByte());
        }

        Deserializer kafkaAvroDeserializer = this.schemaRegistryRepository.getKafkaAvroDeserializer(clusterId);
        ProtobufToJsonDeserializer protobufToJsonDeserializer = customDeserializerRepository.getProtobufToJsonDeserializer(clusterId);

        // key deserializiation
        if(keySchemaId != null) {
            akhqRecord = new AvroKeySchemaRecord(akhqRecord, kafkaAvroDeserializer);
        } else {
            if(protobufToJsonDeserializer != null) {
                var protoBufKey = new ProtoBufKeySchemaRecord(akhqRecord, protobufToJsonDeserializer);
                if(protoBufKey.getKey() != null) {
                    akhqRecord = protoBufKey;
                }
            }
        }

        // value deserializiation
        if(valueSchemaId != null) {
            akhqRecord = new AvroValueSchemaRecord(akhqRecord, kafkaAvroDeserializer);
        } else {
            if (protobufToJsonDeserializer != null) {
                var protoBufValue = new ProtoBufValueSchemaRecord(akhqRecord, protobufToJsonDeserializer);
                if(protoBufValue.getValue() != null) {
                    akhqRecord = protoBufValue;
                }
            }
        }

        return akhqRecord;
    }

    public Record newRecord(ConsumerRecord<byte[], byte[]> record, RecordRepository.BaseOptions options) {
        return this.newRecord(record, options.getClusterId());
    }
}