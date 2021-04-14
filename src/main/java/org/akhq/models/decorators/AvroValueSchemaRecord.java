package org.akhq.models.decorators;

import org.akhq.models.Record;
import org.akhq.utils.AvroToJsonSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Deserializer;

public class AvroValueSchemaRecord extends RecordDecorator {
    private final Deserializer kafkaAvroDeserializer;

    public AvroValueSchemaRecord(Record record, Deserializer kafkaAvroDeserializer) {
        super(record);
        this.kafkaAvroDeserializer = kafkaAvroDeserializer;
    }

    @Override
    public String getValue() {
        if(this.value == null) {
            try {
                GenericRecord record = (GenericRecord) kafkaAvroDeserializer.deserialize(this.getTopic(), this.getBytesValue());
                this.value = AvroToJsonSerializer.toJson(record);
            } catch (Exception exception) {
                this.getExceptions().add(exception.getMessage());

                this.value = new String(this.getBytesValue());
            }
        }

        return this.value;
    }
}
