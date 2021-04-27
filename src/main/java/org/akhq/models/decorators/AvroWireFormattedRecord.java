package org.akhq.models.decorators;

import org.akhq.models.Record;
import org.akhq.repositories.AvroWireFormatConverter;

public class AvroWireFormattedRecord extends Record {
    private final Record record;
    private final AvroWireFormatConverter avroWireFormatConverter;

    public AvroWireFormattedRecord(Record record, AvroWireFormatConverter avroWireFormatConverter) {
        this.record = record;
        this.bytesValue = avroWireFormatConverter.convertValueToWireFormat();
    }
}
