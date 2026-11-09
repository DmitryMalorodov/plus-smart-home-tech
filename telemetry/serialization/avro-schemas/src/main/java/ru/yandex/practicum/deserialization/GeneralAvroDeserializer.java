package ru.yandex.practicum.deserialization;

import org.apache.avro.Schema;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class GeneralAvroDeserializer<T extends SpecificRecordBase> implements Deserializer<T> {
    private final DecoderFactory decoderFactory;
    private final SpecificDatumReader<T> reader;

    public GeneralAvroDeserializer(Schema schema) {
        this(DecoderFactory.get(), schema);
    }

    public GeneralAvroDeserializer(DecoderFactory decoderFactory, Schema schema) {
        this.decoderFactory = decoderFactory;
        this.reader = new SpecificDatumReader<>(schema);
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null || data.length == 0) return null;

        try (ByteArrayInputStream in = new ByteArrayInputStream(data)) {
            BinaryDecoder decoder = decoderFactory.binaryDecoder(in, null);
            return reader.read(null, decoder);
        } catch (IOException e) {
            throw new RuntimeException("Ошибка десериализации Avro-сообщения из топика: " + topic, e);
        }
    }
}
