package ru.yandex.practicum.analyzer.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
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

@Slf4j
@Component
@RequiredArgsConstructor
public class SnapshotProcessor {
    private final ScenarioEngine scenarioEngine;

    private final KafkaConsumer<String, SensorsSnapshotAvro> snapshotConsumer;

    @Value("${spring.kafka.topics.snapshots}")
    private String snapshotsTopic;

    public void start() {
        try {
            log.info("SnapshotProcessor подписывается на топик снапшотов: {}", snapshotsTopic);
            snapshotConsumer.subscribe(List.of(snapshotsTopic));

            while (true) {
                ConsumerRecords<String, SensorsSnapshotAvro> records = snapshotConsumer.poll(Duration.ofMillis(1000));
                if (records.isEmpty()) continue;

                for (ConsumerRecord<String, SensorsSnapshotAvro> record : records) {
                    SensorsSnapshotAvro snapshot = record.value();
                    if (snapshot == null) continue;

                    log.debug("Получен новый снапшот для хаба {}", snapshot.getHubId());
                    scenarioEngine.analyzeAndExecute(snapshot);
                }

                snapshotConsumer.commitSync();
            }
        } catch (WakeupException ignored) {
            log.info("SnapshotProcessor прерван через wakeup().");
        } catch (Exception e) {
            log.error("Критическая ошибка в цикле обработки снапшотов", e);
        } finally {
            snapshotConsumer.close();
            log.info("SnapshotProcessor успешно остановлен.");
        }
    }

    public void stop() {
        if (snapshotConsumer != null) snapshotConsumer.wakeup();
    }
}
