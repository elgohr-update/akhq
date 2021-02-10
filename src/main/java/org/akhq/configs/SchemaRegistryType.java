package org.akhq.configs;

public enum SchemaRegistryType {
    CONFLUENT((byte)0x0),
    TIBCO((byte)0x80);

    private final byte magicByte;

    SchemaRegistryType(final byte magicByte) {
        this.magicByte = magicByte;
    }

    public byte getMagicByte() {
        return magicByte;
    }
}
