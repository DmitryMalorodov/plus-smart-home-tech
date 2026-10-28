package ru.yandex.practicum.aggregator.kafka;

import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

@Slf4j
@Component
public class EventKafkaConsumer implements AutoCloseable {
    private final KafkaConsumer<String, SensorEventAvro> consumer;

    public EventKafkaConsumer(
            @Value("${spring.kafka.bootstrap-servers}") String bootstrapServers,
            @Value("${spring.kafka.consumer.group-id}") String groupId,
            @Value("${spring.kafka.consumer.key-deserializer}") String keyDeserializer,
            @Value("${spring.kafka.consumer.value-deserializer}") String valueDeserializer
    ) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        this.consumer = new KafkaConsumer<>(props);
    }

    public void subscribe(List<String> topics) {
        log.info("EventKafkaConsumer подписывается на топики: {}", topics);
        consumer.subscribe(topics);
    }

    public ConsumerRecords<String, SensorEventAvro> poll(Duration timeout) {
        return consumer.poll(timeout);
    }

    public void commitSync() {
        consumer.commitSync();
    }

    public void wakeup() {
        if (consumer != null) consumer.wakeup();
    }

    @Override
    @PreDestroy
    public void close() {
        if (consumer != null) consumer.close();
    }
}
