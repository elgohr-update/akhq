package org.akhq.models.decorators;

import lombok.Data;

@Data
public class AvroContentTypeMetaData {
    private String subject;
    private int version;

    public static AvroContentTypeMetaData of(String subject, int version) {
        AvroContentTypeMetaData metaData = new AvroContentTypeMetaData();
        metaData.setSubject(subject);
        metaData.setVersion(version);
        return metaData;
    }
}