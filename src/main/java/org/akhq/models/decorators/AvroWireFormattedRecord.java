package org.akhq.models.decorators;

import io.confluent.kafka.schemaregistry.client.SchemaMetadata;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import org.akhq.models.Record;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.regex.Pattern;

public class AvroWireFormattedRecord extends Record {
    public static final Pattern AVRO_CONTENT_TYPE_PATTERN = Pattern.compile("\"?application/vnd\\.(.+)\\.v(\\d+)\\+avro\"?");

    private final SchemaRegistryClient registryClient;
    private final String subject;
    private final int version;
    private final byte magicByte;

    public AvroWireFormattedRecord(Record record, SchemaRegistryClient registryClient, String subject, int version, byte magicByte) {
        super(record);
        this.registryClient = registryClient;
        this.subject = subject;
        this.version = version;
        this.magicByte = magicByte;
    }

    @Override
    public byte[] getBytesValue() {
        try {
            SchemaMetadata schemaMetadata = registryClient.getSchemaMetadata(subject, version);
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            out.write(magicByte);
            out.write(ByteBuffer.allocate(4).putInt(schemaMetadata.getId()).array());
            out.write(this.bytesValue);
            return out.toByteArray();
        } catch (IOException | RestClientException e) {
            // ignore on purpose, dont prepend anything
        }
        return this.bytesValue;
    }
}
