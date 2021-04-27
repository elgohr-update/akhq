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
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.Deserializer;

import javax.inject.Singleton;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.regex.Matcher;

@Singleton
public class RecordFactory {

    private final KafkaModule kafkaModule;
    private final CustomDeserializerRepository customDeserializerRepository;
    private final SchemaRegistryRepository schemaRegistryRepository;

    public RecordFactory(KafkaModule kafkaModule,
                         CustomDeserializerRepository customDeserializerRepository,
                         SchemaRegistryRepository schemaRegistryRepository) {
        this.kafkaModule = kafkaModule;
        this.customDeserializerRepository = customDeserializerRepository;
        this.schemaRegistryRepository = schemaRegistryRepository;
    }

    public Record newRecord(ConsumerRecord<byte[], byte[]> record, String clusterId) {
        SchemaRegistryType schemaRegistryType = this.schemaRegistryRepository.getSchemaRegistryType(clusterId);
        SchemaRegistryClient registryClient = kafkaModule.getRegistryClient(clusterId);
        Integer keySchemaId = schemaRegistryRepository.determineAvroSchemaForPayload(schemaRegistryType, record.key());
        Integer valueSchemaId = schemaRegistryRepository.determineAvroSchemaForPayload(schemaRegistryType, record.value());

        // base record (default: string)
        Record akhqRecord = new Record(record, keySchemaId, valueSchemaId);

        // avro wire format
        Iterator<Header> contentTypeIter = record.headers().headers("contentType").iterator();
        byte magicByte = schemaRegistryType.getMagicByte();
        byte[] value = record.value();
        if (contentTypeIter.hasNext() && value.length > 0 && ByteBuffer.wrap(value).get() != magicByte) {
            String headerValue = new String(contentTypeIter.next().value());
            Matcher matcher = AvroWireFormattedRecord.AVRO_CONTENT_TYPE_PATTERN.matcher(headerValue);
            if (matcher.matches()) {
                String subject = matcher.group(1);
                int version = Integer.parseInt(matcher.group(2));
                akhqRecord = new AvroWireFormattedRecord(akhqRecord, registryClient, subject, version, magicByte);
            }
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