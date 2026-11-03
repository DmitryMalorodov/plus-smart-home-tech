package ru.yandex.practicum.analyzer.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.analyzer.service.ScenarioEngine;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

@Slf4j
@Component
@RequiredArgsConstructor
public class SnapshotProcessor {
    private final ScenarioEngine scenarioEngine;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    @Value("${spring.kafka.topics.snapshots}")
    private String snapshotsTopic;

    private KafkaConsumer<String, SensorsSnapshotAvro> consumer;

    public void start() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "analyzer-snapshots-group");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        this.consumer = new KafkaConsumer<>(
                props,
                new org.apache.kafka.common.serialization.StringDeserializer(),
                new ru.yandex.practicum.deserialization.SensorsSnapshotDeserializer()
        );

        try {
            log.info("SnapshotProcessor подписывается на топик снапшотов: {}", snapshotsTopic);
            consumer.subscribe(List.of(snapshotsTopic));

            while (true) {
                ConsumerRecords<String, SensorsSnapshotAvro> records = consumer.poll(Duration.ofMillis(1000));
                if (records.isEmpty()) continue;

                for (ConsumerRecord<String, SensorsSnapshotAvro> record : records) {
                    SensorsSnapshotAvro snapshot = record.value();
                    if (snapshot == null) continue;

                    log.debug("Получен новый снапшот для хаба {}", snapshot.getHubId());
                    scenarioEngine.analyzeAndExecute(snapshot);
                }

                consumer.commitSync();
            }
        } catch (WakeupException ignored) {
            log.info("SnapshotProcessor прерван через wakeup().");
        } catch (Exception e) {
            log.error("Критическая ошибка в цикле обработки снапшотов", e);
        } finally {
            try {
                consumer.commitSync();
            } catch (Exception ignored) {}
            consumer.close();
            log.info("SnapshotProcessor успешно остановлен.");
        }
    }

    public void stop() {
        if (consumer != null) consumer.wakeup();
    }
}
