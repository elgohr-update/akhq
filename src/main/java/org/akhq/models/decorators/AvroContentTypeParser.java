package org.akhq.models.decorators;

import lombok.Data;
import org.akhq.configs.SchemaRegistryType;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;

import javax.inject.Singleton;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Optional;
import java.util.regex.Matcher;

@Singleton
public class AvroContentTypeParser {

    public Optional<AvroContentTypeMetaData> parseAvroContentTypeMetaData(ConsumerRecord<byte[], byte[]> record, SchemaRegistryType schemaRegistryType) {
        Iterator<Header> contentTypeIter = record.headers().headers("contentType").iterator();
        byte magicByte = schemaRegistryType.getMagicByte();
        byte[] value = record.value();
        if (contentTypeIter.hasNext() && value.length > 0 && ByteBuffer.wrap(value).get() != magicByte) {
            String headerValue = new String(contentTypeIter.next().value());
            Matcher matcher = AvroWireFormattedRecord.AVRO_CONTENT_TYPE_PATTERN.matcher(headerValue);
            if (matcher.matches()) {
                String subject = matcher.group(1);
                int version = Integer.parseInt(matcher.group(2));
                AvroContentTypeMetaData metaData = new AvroContentTypeMetaData();
                metaData.setSubject(subject);
                metaData.setVersion(version);
                return Optional.of(metaData);
            }
        }
        return Optional.empty();
    }
}
