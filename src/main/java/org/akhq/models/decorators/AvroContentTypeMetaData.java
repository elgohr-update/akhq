package org.akhq.models.decorators;

import lombok.Data;

@Data
public class AvroContentTypeMetaData {
    private String subject;
    private int version;
}