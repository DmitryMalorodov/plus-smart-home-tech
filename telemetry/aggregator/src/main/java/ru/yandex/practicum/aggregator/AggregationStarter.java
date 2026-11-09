package ru.yandex.practicum.aggregator;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.aggregator.kafka.EventKafkaConsumer;
import ru.yandex.practicum.aggregator.kafka.EventKafkaProducer;
import ru.yandex.practicum.aggregator.kafka.KafkaConfig;
import ru.yandex.practicum.aggregator.service.SnapshotAggregator;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

import java.time.Duration;
import java.util.List;

@Slf4j
@Component
public class AggregationStarter {
    private final KafkaConfig kafkaConfig;
    private final SnapshotAggregator snapshotAggregator;
    private final String inputTopic;
    private final String outputTopic;
    private volatile EventKafkaConsumer activeConsumer;

    public AggregationStarter(
            KafkaConfig kafkaConfig,
            SnapshotAggregator snapshotAggregator,
            @Value("${spring.kafka.topics.sensors}") String inputTopic,
            @Value("${spring.kafka.topics.snapshots}") String outputTopic
    ) {
        this.kafkaConfig = kafkaConfig;
        this.snapshotAggregator = snapshotAggregator;
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
    }

    public void start() {
        EventKafkaConsumer consumer = kafkaConfig.eventKafkaConsumer();
        EventKafkaProducer producer = kafkaConfig.eventKafkaProducer();
        this.activeConsumer = consumer;

        try {
            log.info("Подписываемся на топик событий датчиков: {}", inputTopic);
            consumer.subscribe(List.of(inputTopic));

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Получен сигнал остановки, вызываем consumer.wakeup()");
                consumer.wakeup();
            }));

            while (true) {
                ConsumerRecords<String, SensorEventAvro> records = consumer.poll(Duration.ofMillis(1000));

                if (records.isEmpty()) continue;

                for (ConsumerRecord<String, SensorEventAvro> record : records) {
                    SensorEventAvro event = record.value();
                    if (event == null) continue;

                    snapshotAggregator.updateState(event).ifPresent(snapshot -> {
                        String hubId = snapshot.getHubId();
                        log.debug("Отправляем обновленный снапшот для хаба: {}", hubId);

                        producer.send(
                                outputTopic,
                                hubId,
                                snapshot,
                                snapshot.getTimestamp()
                        );
                    });
                }

                producer.flush();
                consumer.commitSync();
            }

        } catch (WakeupException ignored) {
            log.info("Получено исключение WakeupException. Запускается процесс корректного закрытия ресурсов.");
        } catch (Exception e) {
            log.error("Ошибка во время обработки событий от датчиков", e);
        } finally {
            try {
                log.info("Принудительно сбрасываем оставшиеся буферы продюсера");
                producer.flush();
                log.info("Фиксируем последние обработанные смещения");
                consumer.commitSync();
            } catch (Exception e) {
                log.error("Ошибка при финальной фиксации смещений или сбросе буферов", e);
            } finally {
                log.info("Закрываем консьюмер");
                consumer.close();
                log.info("Закрываем продюсер");
                producer.close();
                this.activeConsumer = null;
                log.info("Сервис Aggregator успешно остановлен.");
            }
        }
    }
}