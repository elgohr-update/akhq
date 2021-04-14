package org.akhq.models.decorators;

import org.akhq.models.Record;
import org.akhq.utils.ProtobufToJsonDeserializer;

public class ProtoBufKeySchemaRecord extends RecordDecorator {
    private final ProtobufToJsonDeserializer protoBufDeserializer;

    public ProtoBufKeySchemaRecord(Record record, ProtobufToJsonDeserializer protoBufDeserializer) {
        super(record);
        this.protoBufDeserializer = protoBufDeserializer;
    }

    @Override
    public String getKey() {
        if(this.key == null) {
            try {
                String record = protoBufDeserializer.deserialize(this.getTopic(), this.getBytesKey(), true);
                if (record != null) {
                    this.key = record;
                }
            } catch (Exception exception) {
                this.getExceptions().add(exception.getMessage());

                this.key = new String(this.getBytesKey());
            }
        }

        return this.key;
    }
}
