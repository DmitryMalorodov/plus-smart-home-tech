package ru.yandex.practicum.aggregator.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorStateAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorsSnapshotAvro;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Slf4j
@Component
public class SnapshotAggregator {
    private final Map<String, SensorsSnapshotAvro> snapshots = new HashMap<>();

    public Optional<SensorsSnapshotAvro> updateState(SensorEventAvro event) {
        String hubId = event.getHubId();
        String sensorId = event.getId();

        SensorsSnapshotAvro snapshot = snapshots.computeIfAbsent(hubId, id -> SensorsSnapshotAvro.newBuilder()
                .setHubId(id)
                .setTimestamp(event.getTimestamp())
                .setSensorsState(new HashMap<>())
                .build());

        SensorStateAvro oldState = snapshot.getSensorsState().get(sensorId);

        SensorStateAvro newState = SensorStateAvro.newBuilder()
                .setTimestamp(event.getTimestamp())
                .setData(event.getPayload())
                .build();

        if (oldState != null) {
            if (event.getTimestamp().isBefore(oldState.getTimestamp())) {
                log.debug("Событие датчика {} пропущено: таймстемп из прошлого", sensorId);
                return Optional.empty();
            }

            if (oldState.getData().toString().equals(event.getPayload().toString())) {
                log.debug("Событие датчика {} проигнорировано: данные не изменились", sensorId);
                return Optional.empty();
            }
        }

        snapshot.getSensorsState().put(sensorId, newState);
        snapshot.setTimestamp(event.getTimestamp());

        log.info("Снапшот хаба {} успешно обновлен датчиком {}", hubId, sensorId);
        return Optional.of(snapshot);
    }
}
