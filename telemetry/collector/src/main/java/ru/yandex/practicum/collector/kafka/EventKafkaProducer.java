package ru.yandex.practicum.collector.kafka;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.Instant;
import java.util.Properties;

@Slf4j
@Component
public class EventKafkaProducer implements AutoCloseable {
    private final Producer<String, byte[]> producer;

    public EventKafkaProducer(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${spring.kafka.producer.key-serializer}") String keySerializer,
            @Value("${spring.kafka.producer.value-serializer}") String valueSerializer
    ) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializer);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializer);
        producer = new KafkaProducer<>(props);
    }

    public void send(String topic, String key, SpecificRecordBase avroRecord, Instant timestamp) {
        try {
            byte[] binaryData = serialize(avroRecord);
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, null, timestamp.toEpochMilli(), key, binaryData);

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    log.error("Ошибка при записи в топик {} для ключа {}", topic, key, exception);
                } else {
                    log.debug("Сообщение успешно записано: топик={}, партиция={}, офсет={}",
                            metadata.topic(), metadata.partition(), metadata.offset());
                }
            });

        } catch (IOException e) {
            throw new RuntimeException("Не удалось сжать Avro-объект в массив байт", e);
        }
    }

    private byte[] serialize(SpecificRecordBase record) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        @SuppressWarnings("unchecked")
        SpecificDatumWriter<SpecificRecordBase> writer = new SpecificDatumWriter<>((Class<SpecificRecordBase>) record.getClass());
        writer.write(record, encoder);
        encoder.flush();
        return out.toByteArray();
    }

    @Override
    @PreDestroy
    public void close() {
        if (producer != null) producer.close();
    }
}

