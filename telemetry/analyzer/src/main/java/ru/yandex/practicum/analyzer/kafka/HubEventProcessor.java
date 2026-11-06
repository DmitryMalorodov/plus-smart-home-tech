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
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.analyzer.model.*;
import ru.yandex.practicum.analyzer.repository.ActionRepository;
import ru.yandex.practicum.analyzer.repository.ConditionRepository;
import ru.yandex.practicum.analyzer.repository.ScenarioRepository;
import ru.yandex.practicum.analyzer.repository.SensorRepository;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.time.Duration;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Component
@RequiredArgsConstructor
public class HubEventProcessor implements Runnable {
    private final SensorRepository sensorRepository;
    private final ScenarioRepository scenarioRepository;
    private final ConditionRepository conditionRepository;
    private final ActionRepository actionRepository;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    @Value("${spring.kafka.topics.hubs}")
    private String hubsTopic;

    private KafkaConsumer<String, HubEventAvro> consumer;

    @Override
    public void run() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "analyzer-hub-events-group");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        this.consumer = new KafkaConsumer<>(
                props,
                new org.apache.kafka.common.serialization.StringDeserializer(),
                new ru.yandex.practicum.deserialization.HubEventDeserializer()
        );

        try {
            log.info("HubEventProcessor подписывается на топик: {}", hubsTopic);
            consumer.subscribe(List.of(hubsTopic));

            while (true) {
                ConsumerRecords<String, HubEventAvro> records = consumer.poll(Duration.ofMillis(1000));

                if (records.isEmpty()) continue;
                boolean batchProcessedSuccessfully = true;

                for (ConsumerRecord<String, HubEventAvro> record : records) {
                    HubEventAvro event = record.value();
                    if (event == null) continue;
                    try {
                        handleHubEvent(event);
                    } catch (Exception e) {
                        log.error("Ошибка при обработке события хаба: {}", event, e);
                        batchProcessedSuccessfully = false;
                        break;
                    }
                }

                if (batchProcessedSuccessfully) {
                    try {
                        consumer.commitSync();
                    } catch (Exception e) {
                        log.error("Не удалось закоммитить оффсеты в Kafka", e);
                    }
                } else {
                    throw new RuntimeException("Пакет сообщений не был полностью обработан. Оффсеты не закоммичены.");
                }
            }
        } catch (WakeupException ignored) {
            log.info("HubEventProcessor прерван через wakeup().");
        } finally {
            consumer.close();
            log.info("HubEventProcessor успешно остановлен.");
        }
    }

    @Transactional
    public void handleHubEvent(HubEventAvro event) {
        String hubId = event.getHubId();
        Object payload = event.getPayload();

        if (payload instanceof DeviceAddedEventAvro deviceAdded) {
            log.info("Добавление датчика {} в хаб {}", deviceAdded.getId(), hubId);

            if (!sensorRepository.existsById(deviceAdded.getId())) {
                Sensor sensor = Sensor.builder()
                        .id(deviceAdded.getId())
                        .hubId(hubId)
                        .build();
                sensorRepository.save(sensor);
                sensorRepository.flush();
                log.info("Датчик {} успешно сохранен.", deviceAdded.getId());
            }

        } else if (payload instanceof DeviceRemovedEventAvro deviceRemoved) {
            log.info("Удаление датчика {} из хаба {}", deviceRemoved.getId(), hubId);
            sensorRepository.findByIdAndHubId(deviceRemoved.getId(), hubId)
                    .ifPresent(sensor -> {
                        sensorRepository.delete(sensor);
                        sensorRepository.flush();
                    });

        } else if (payload instanceof ScenarioAddedEventAvro scenarioAdded) {
            log.info("Добавление/обновление сценария '{}' для хаба {}", scenarioAdded.getName(), hubId);

            // 1. Удаляем старый сценарий с таким же именем, если он был
            scenarioRepository.findByHubIdAndName(hubId, scenarioAdded.getName())
                    .ifPresent(scenarioRepository::delete);

            // 2. Создаем и сохраняем сам сценарий, чтобы получить его ID
            Scenario scenario = Scenario.builder()
                    .hubId(hubId)
                    .name(scenarioAdded.getName())
                    .build();
            scenario = scenarioRepository.save(scenario);

            // 3. Собираем уникальные ID всех датчиков
            Set<String> allSensorIds = new java.util.HashSet<>();
            if (scenarioAdded.getConditions() != null) {
                scenarioAdded.getConditions().forEach(c -> allSensorIds.add(c.getSensorId()));
            }
            if (scenarioAdded.getActions() != null) {
                scenarioAdded.getActions().forEach(a -> allSensorIds.add(a.getSensorId()));
            }

            // 4. Гарантируем существование датчиков в БД
            List<Sensor> existingSensors = sensorRepository.findAllById(allSensorIds);
            Map<String, Sensor> sensorMap = existingSensors.stream()
                    .collect(Collectors.toMap(Sensor::getId, Function.identity()));

            List<Sensor> sensorsToSave = new ArrayList<>();
            for (String sensorId : allSensorIds) {
                if (!sensorMap.containsKey(sensorId)) {
                    Sensor newSensor = Sensor.builder()
                            .id(sensorId)
                            .hubId(hubId)
                            .build();
                    sensorsToSave.add(newSensor);
                    sensorMap.put(sensorId, newSensor);
                }
            }
            if (!sensorsToSave.isEmpty()) {
                sensorRepository.saveAll(sensorsToSave);
            }

            final Scenario finalScenario = scenario;

            // 5. Маппим и сохраняем условия сценария
            List<Condition> conditionsToSave = scenarioAdded.getConditions().stream().map(c ->
                    Condition.builder()
                            .type(ConditionType.valueOf(c.getType().name()))
                            .operation(ConditionOperation.valueOf(c.getOperation().name()))
                            .value(c.getValue() instanceof Integer ? (Integer) c.getValue() : ((Boolean) c.getValue() ? 1 : 0))
                            .build()
            ).toList();

            List<Condition> savedConditions = conditionRepository.saveAll(conditionsToSave);

            List<ScenarioCondition> conditions = new ArrayList<>();
            for (int i = 0; i < scenarioAdded.getConditions().size(); i++) {
                var c = scenarioAdded.getConditions().get(i);
                Condition cond = savedConditions.get(i);
                Sensor sensor = sensorMap.get(c.getSensorId());

                if (sensor == null) {
                    throw new IllegalStateException("Датчик не найден в локальном контексте: " + c.getSensorId());
                }

                conditions.add(ScenarioCondition.builder()
                        .id(new ScenarioCondition.Id(finalScenario.getId(), sensor.getId(), cond.getId()))
                        .scenario(finalScenario)
                        .sensor(sensor)
                        .condition(cond)
                        .build());
            }

            // 6. Маппим и сохраняем действия сценария
            List<Action> actionsToSave = scenarioAdded.getActions().stream().map(a ->
                    Action.builder()
                            .type(ActionType.valueOf(a.getType().name()))
                            .value(a.getValue() != null ? a.getValue() : null)
                            .build()
            ).toList();

            List<Action> savedActions = actionRepository.saveAll(actionsToSave);

            List<ScenarioAction> actions = new ArrayList<>();
            for (int i = 0; i < scenarioAdded.getActions().size(); i++) {
                var a = scenarioAdded.getActions().get(i);
                Action act = savedActions.get(i);
                Sensor sensor = sensorMap.get(a.getSensorId());

                if (sensor == null) {
                    throw new IllegalStateException("Датчик не найден в локальном контексте: " + a.getSensorId());
                }

                actions.add(ScenarioAction.builder()
                        .id(new ScenarioAction.Id(finalScenario.getId(), sensor.getId(), act.getId()))
                        .scenario(finalScenario)
                        .sensor(sensor)
                        .action(act)
                        .build());
            }

            // 7. Обновляем списки в сценарии и сохраняем финальное состояние
            finalScenario.setConditions(conditions);
            finalScenario.setActions(actions);
            scenarioRepository.save(finalScenario);
            scenarioRepository.flush();

            log.info("Сценарий '{}' успешно сохранен.", finalScenario.getName());

        } else if (payload instanceof ScenarioRemovedEventAvro scenarioRemoved) {
            log.info("Удаление сценария '{}' из хаба {}", scenarioRemoved.getName(), hubId);
            scenarioRepository.findByHubIdAndName(hubId, scenarioRemoved.getName())
                    .ifPresent(scenario -> {
                        scenarioRepository.delete(scenario);
                        scenarioRepository.flush();
                    });
        }
    }

    public void stop() {
        if (consumer != null) consumer.wakeup();
    }
}