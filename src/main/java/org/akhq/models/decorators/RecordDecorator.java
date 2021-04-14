package org.akhq.models.decorators;

import org.akhq.models.Record;
import org.apache.kafka.common.record.TimestampType;

import java.time.ZonedDateTime;
import java.util.List;
import java.util.Map;

/**
 * This abstract class defines a Record decorator
 */
public abstract class RecordDecorator extends Record {

    /** Wrapped record instance to decorate */
    private Record wrapped;

    /**
     * Constructor takes a record object and uses it as a reference
     */
    public RecordDecorator(Record record) {
        this.wrapped = record;
    }

    @Override
    public String getTopic() {
        return this.wrapped.getTopic();
    }

    @Override
    public int getPartition() {
        return this.wrapped.getPartition();
    }

    @Override
    public long getOffset() {
        return this.wrapped.getOffset();
    }

    @Override
    public ZonedDateTime getTimestamp() {
        return this.wrapped.getTimestamp();
    }

    @Override
    public TimestampType getTimestampType() {
        return this.wrapped.getTimestampType();
    }

    @Override
    public Integer getKeySchemaId() {
        return this.wrapped.getKeySchemaId();
    }

    @Override
    public Integer getValueSchemaId() {
        return this.wrapped.getValueSchemaId();
    }

    @Override
    public Map<String, String> getHeaders() {
        return this.wrapped.getHeaders();
    }

    @Override
    public byte[] getBytesKey() {
        return this.wrapped.getBytesKey();
    }

    @Override
    public String getKey() {
        return this.wrapped.getKey();
    }

    @Override
    public byte[] getBytesValue() {
        return this.wrapped.getBytesValue();
    }

    @Override
    public String getValue() {
        return this.wrapped.getValue();
    }

    @Override
    public List<String> getExceptions() {
        return this.wrapped.getExceptions();
    }
}
