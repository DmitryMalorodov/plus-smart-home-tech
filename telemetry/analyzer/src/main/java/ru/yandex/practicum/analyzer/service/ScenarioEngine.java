package ru.yandex.practicum.analyzer.service;

import com.google.protobuf.Timestamp;
import lombok.extern.slf4j.Slf4j;
import net.devh.boot.grpc.client.inject.GrpcClient;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.analyzer.model.Scenario;
import ru.yandex.practicum.analyzer.model.ScenarioAction;
import ru.yandex.practicum.analyzer.repository.ScenarioRepository;
import ru.yandex.practicum.analyzer.util.SensorDataExtractor;
import ru.yandex.practicum.grpc.telemetry.event.ActionTypeProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionRequest;
import ru.yandex.practicum.grpc.telemetry.hubrouter.HubRouterControllerGrpc.HubRouterControllerBlockingStub;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.time.Instant;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
public class ScenarioEngine {
    private final ScenarioRepository scenarioRepository;
    private final HubRouterControllerBlockingStub hubRouterClient;

    public ScenarioEngine(ScenarioRepository scenarioRepository,
                          @GrpcClient("hub-router") HubRouterControllerBlockingStub hubRouterClient) {
        this.scenarioRepository = scenarioRepository;
        this.hubRouterClient = hubRouterClient;
    }

    @Transactional(readOnly = true)
    public void analyzeAndExecute(SensorsSnapshotAvro snapshot) {
        String hubId = snapshot.getHubId();
        Map<String, SensorStateAvro> sensorsState = snapshot.getSensorsState();

        if (sensorsState == null || sensorsState.isEmpty()) {
            log.debug("Снапшот хаба {} пустой, пропускаем анализ.", hubId);
            return;
        }

        // 1. Загружаем сценарии для текущего хаба
        List<Scenario> scenarios = scenarioRepository.findByHubId(hubId);
        if (scenarios.isEmpty()) {
            log.debug("Для хаба {} не найдено сохраненных сценариев.", hubId);
            return;
        }

        // 2. Алгоритм фильтрации и выполнения
        scenarios.stream()
                .filter(scenario -> checkConditions(scenario, sensorsState))
                .flatMap(scenario -> scenario.getActions().stream())
                .forEach(action -> executeAction(hubId, action));
    }

    private boolean checkConditions(Scenario scenario, Map<String, SensorStateAvro> sensorsState) {
        if (scenario.getConditions().isEmpty()) {
            return false;
        }

        // Сценарий активен, если КАЖДОЕ его условие истинно
        return scenario.getConditions().stream().allMatch(cond -> {
            SensorStateAvro sensorState = sensorsState.get(cond.getSensor().getId());
            if (sensorState == null) {
                return false;
            }

            Integer currentValue = SensorDataExtractor.extractValue(sensorState.getData(), cond.getCondition().getType());
            if (currentValue == null) {
                return false;
            }

            Integer targetValue = cond.getCondition().getValue();

            return switch (cond.getCondition().getOperation()) {
                case EQUALS -> currentValue.equals(targetValue);
                case GREATER_THAN -> currentValue > targetValue;
                case LOWER_THAN -> currentValue < targetValue;
            };
        });
    }

    private void executeAction(String hubId, ScenarioAction scenarioAction) {
        var actionEntity = scenarioAction.getAction();

        // Формируем внутреннее сообщение DeviceActionProto
        DeviceActionProto.Builder actionProtoBuilder = DeviceActionProto.newBuilder()
                .setSensorId(scenarioAction.getSensor().getId())
                .setType(ActionTypeProto.valueOf(actionEntity.getType().name()));

        if (actionEntity.getValue() != null) {
            actionProtoBuilder.setValue(actionEntity.getValue());
        }

        Instant now = Instant.now();
        Timestamp timestamp = Timestamp.newBuilder()
                .setSeconds(now.getEpochSecond())
                .setNanos(now.getNano())
                .build();

        // Формируем корневой запрос DeviceActionRequest
        DeviceActionRequest request = DeviceActionRequest.newBuilder()
                .setHubId(hubId)
                .setScenarioName(scenarioAction.getScenario().getName())
                .setAction(actionProtoBuilder.build())
                .setTimestamp(timestamp)
                .build();

        try {
            log.info("Отправка команды для хаба {} по сценарию '{}' на датчик {}",
                    hubId, scenarioAction.getScenario().getName(), scenarioAction.getSensor().getId());
            hubRouterClient.handleDeviceAction(request);
        } catch (Exception e) {
            log.error("Не удалось отправить команду gRPC для хаба {} по сценарию '{}'",
                    hubId, scenarioAction.getScenario().getName(), e);
        }
    }
}
