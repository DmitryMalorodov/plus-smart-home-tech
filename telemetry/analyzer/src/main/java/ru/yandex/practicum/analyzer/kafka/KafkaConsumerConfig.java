package ru.yandex.practicum.analyzer.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import ru.yandex.practicum.deserialization.HubEventDeserializer;
import ru.yandex.practicum.deserialization.SensorsSnapshotDeserializer;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.util.Properties;

@Configuration
public class KafkaConsumerConfig {

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.groups.hubs}")
    private String hubsGroupId;

    @Value("${spring.kafka.groups.snapshots}")
    private String snapshotsGroupId;

    @Bean
    public KafkaConsumer<String, HubEventAvro> hubEventConsumer() {
        Properties props = createBaseProperties(hubsGroupId);
        return new KafkaConsumer<>(
                props,
                new StringDeserializer(),
                new HubEventDeserializer()
        );
    }

    @Bean
    public KafkaConsumer<String, SensorsSnapshotAvro> snapshotConsumer() {
        Properties props = createBaseProperties(snapshotsGroupId);
        return new KafkaConsumer<>(
                props,
                new StringDeserializer(),
                new SensorsSnapshotDeserializer()
        );
    }

    private Properties createBaseProperties(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return props;
    }
}
