package org.akhq.models.decorators;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import lombok.extern.slf4j.Slf4j;
import org.akhq.models.Record;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Converts an avro payload to the kafka avro wire format (https://docs.confluent.io/current/schema-registry/serializer-formatter.html#wire-format)
 * Some producers (like Spring Cloud Stream) do write this wire format, but use the raw avro binary encoding (without magic byte and schema id)
 * and put the reference to the schema in a header field. This converter will add the magic byte and schema id to the byte[] to
 * be wire format compatible if the following conditions are met:
 * - magic byte is not already present
 * - schema reference (subject and version) can be found in the message header
 * - schema can be fetch from the registry
 */
@Slf4j
public class AvroWireFormattedRecord extends Record {
    private final SchemaRegistryClient registryClient;
    private final AvroContentTypeMetaData avroContentTypeMetaData;
    private final byte magicByte;

    public AvroWireFormattedRecord(Record record, SchemaRegistryClient registryClient, AvroContentTypeMetaData avroContentTypeMetaData, byte magicByte) {
        super(record);
        this.registryClient = registryClient;
        this.avroContentTypeMetaData = avroContentTypeMetaData;
        this.magicByte = magicByte;
    }

    @Override
    public byte[] getBytesValue() {
        if(this.bytesValue != null && this.bytesValue.length > 1 && avroContentTypeMetaData != null) {
            try(ByteArrayOutputStream out = new ByteArrayOutputStream()) {
                SchemaMetadata schemaMetadata = registryClient.getSchemaMetadata(avroContentTypeMetaData.getSubject(), avroContentTypeMetaData.getVersion());
                out.write(magicByte);
                out.write(ByteBuffer.allocate(4).putInt(schemaMetadata.getId()).array());
                out.write(this.bytesValue);
                return out.toByteArray();
            } catch (IOException | RestClientException e) {
                if(log.isTraceEnabled()) {
                    log.debug("Failure when trying to parse schema metadata of subject {} and version {}.",
                        avroContentTypeMetaData.getSubject(),
                        avroContentTypeMetaData.getVersion()
                    );
                }
            }
        }

        return this.bytesValue;
    }
}
