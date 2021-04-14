package org.akhq.models.decorators;

import org.akhq.models.Record;
import org.akhq.utils.Album;
import org.akhq.utils.AlbumProto;
import org.akhq.utils.ProtobufToJsonDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class ProtoBufKeySchemaRecordTest {

    @Test
    @Tag("UnitTest")
    public void testGetKeyProtobufSerialized() {

        // Test data
        AlbumProto.Album aProtoBufAlbum = aProtoBufAlbumObject();
        String expectedAlbum = "{\n" +
                "  \"title\": \"Origins\",\n" +
                "  \"artist\": [\"Imagine Dragons\"],\n" +
                "  \"releaseYear\": 2018,\n" +
                "  \"songTitle\": [\"Birds\", \"Zero\", \"Natural\", \"Machine\"]\n" +
                "}";

        // GIVEN a record with a protobuf serialized key
        byte[] keyBytes = aProtoBufAlbum.toByteArray();
        byte[] valueBytes = null;

        ConsumerRecord<byte[], byte[]> kafkaRecord = new ConsumerRecord<>("topic", 0, 0, keyBytes, valueBytes);
        Record record = new Record(kafkaRecord, 1, 2);

        // AND this record is decorated
        ProtobufToJsonDeserializer aMockedProtobufDeserializer = Mockito.mock(ProtobufToJsonDeserializer.class); // NOTICE: protobufToJsonDeserializer does not implement Deserializer interface
        Mockito.when(aMockedProtobufDeserializer.deserialize(Mockito.any(), Mockito.any(), Mockito.anyBoolean())).thenReturn(expectedAlbum);
        record = new ProtoBufKeySchemaRecord(record, aMockedProtobufDeserializer);

        // EXPECT getKey() returns a String with the json content
        assertThat(record.getKey(), is(expectedAlbum));
    }

    @Test
    @Tag("UnitTest")
    public void testGetKeyProtoBufSerializedFallback() {

        // Test data
        AlbumProto.Album aProtoBufAlbum = aProtoBufAlbumObject();

        // GIVEN a record with protobuf serialized key
        byte[] keyBytes = aProtoBufAlbum.toByteArray();
        byte[] valueBytes = null;

        ConsumerRecord<byte[], byte[]> kafkaRecord = new ConsumerRecord<>("topic", 0, 0, keyBytes, valueBytes);
        Record record = new Record(kafkaRecord, 1, 2);

        // AND this record is decorated
        ProtobufToJsonDeserializer aMockedProtobufDeserializer = Mockito.mock(ProtobufToJsonDeserializer.class); // NOTICE: protobufToJsonDeserializer does not implement Deserializer interface
        record = new ProtoBufKeySchemaRecord(record, aMockedProtobufDeserializer);

        // WHEN protobuf deserializer throws an exception
        Mockito.when(aMockedProtobufDeserializer.deserialize(Mockito.any(), Mockito.any(), Mockito.anyBoolean()))
                .then(invocation -> { throw new NullPointerException("exception"); });

        // EXPECT getKey() returns a string representation of the key bytes array
        assertThat(record.getKey(), is(new String(aProtoBufAlbum.toByteArray())));
    }

    /**
     * Method returns a protobuf album object
     */
    private AlbumProto.Album aProtoBufAlbumObject() {
        List<String> artists = Collections.singletonList("Imagine Dragons");
        List<String> songTitles = Arrays.asList("Birds", "Zero", "Natural", "Machine");
        Album album = new Album("Origins", artists, 2018, songTitles);
        return AlbumProto.Album.newBuilder()
                .setTitle(album.getTitle())
                .addAllArtist(album.getArtists())
                .setReleaseYear(album.getReleaseYear())
                .addAllSongTitle(album.getSongsTitles())
                .build();
    }
}
