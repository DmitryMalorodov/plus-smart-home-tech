package ru.yandex.practicum.analyzer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.ConfigurableApplicationContext;
import ru.yandex.practicum.analyzer.kafka.HubEventProcessor;
import ru.yandex.practicum.analyzer.kafka.SnapshotProcessor;

@Slf4j
@SpringBootApplication
@ConfigurationPropertiesScan
public class AnalyzerApplication {

    public static void main(String[] args) {
        ConfigurableApplicationContext context =
                SpringApplication.run(AnalyzerApplication.class, args);

        final HubEventProcessor hubEventProcessor =
                context.getBean(HubEventProcessor.class);
        final SnapshotProcessor snapshotProcessor =
                context.getBean(SnapshotProcessor.class);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Сигнал завершения работы приложения. Останавливаем Kafka консьюмеры...");
            hubEventProcessor.stop();
            snapshotProcessor.stop();
        }));

        Thread hubEventsThread = new Thread(hubEventProcessor);
        hubEventsThread.setName("HubEventHandlerThread");
        hubEventsThread.start();

        log.info("Запуск основного цикла обработки снапшотов");
        snapshotProcessor.start();
    }
}
