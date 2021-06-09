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
                Object toType = kafkaAvroDeserializer.deserialize(this.getTopic(), this.getBytesValue());

                //for primitive avro type
                if (!(toType instanceof GenericRecord)){
                    return String.valueOf(toType);
                }

                GenericRecord record = (GenericRecord) toType;
                this.value = AvroToJsonSerializer.toJson(record);
            } catch (Exception exception) {
                this.getExceptions().add(exception.getMessage());

                this.value = new String(this.getBytesValue());
            }
        }

        return this.value;
    }
}
