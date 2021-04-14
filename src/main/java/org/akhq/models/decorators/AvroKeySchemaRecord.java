package org.akhq.models.decorators;

import org.akhq.models.Record;
import org.akhq.utils.AvroToJsonSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;

public class AvroKeySchemaRecord extends RecordDecorator {
    private final Deserializer kafkaAvroDeserializer;

    public AvroKeySchemaRecord(Record record, Deserializer kafkaAvroDeserializer) {
        super(record);
        this.kafkaAvroDeserializer = kafkaAvroDeserializer;
    }

    @Override
    public String getKey() {
        if(this.key != null) {
            return this.key;
        }

        try {
            GenericRecord record = (GenericRecord) kafkaAvroDeserializer.deserialize(this.getTopic(), this.getBytesKey());
            return AvroToJsonSerializer.toJson(record);
        } catch (Exception exception) {
            this.getExceptions().add(exception.getMessage());

            return new String(this.getBytesKey());
        }
    }
}
