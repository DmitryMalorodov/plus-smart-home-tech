package ru.yandex.practicum.collector.mapper;

import ru.yandex.practicum.collector.dto.hubs.HubEvent;
import ru.yandex.practicum.collector.dto.hubs.device.DeviceAddedEvent;
import ru.yandex.practicum.collector.dto.hubs.device.DeviceRemovedEvent;
import ru.yandex.practicum.collector.dto.hubs.scenario.DeviceAction;
import ru.yandex.practicum.collector.dto.hubs.scenario.ScenarioAddedEvent;
import ru.yandex.practicum.collector.dto.hubs.scenario.ScenarioCondition;
import ru.yandex.practicum.collector.dto.hubs.scenario.ScenarioRemovedEvent;
import ru.yandex.practicum.collector.dto.sensors.*;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.util.Collections;
import java.util.stream.Collectors;

public class EventMapper {

    public static SensorEventAvro toAvro(SensorEvent event) {
        if (event == null) return null;

        Object payload = switch (event) {
            case ClimateSensorEvent e -> ClimateSensorAvro.newBuilder()
                    .setTemperatureC(e.getTemperatureC())
                    .setHumidity(e.getHumidity())
                    .setCo2Level(e.getCo2Level())
                    .build();

            case LightSensorEvent e -> LightSensorAvro.newBuilder()
                    .setLinkQuality(e.getLinkQuality())
                    .setLuminosity(e.getLuminosity())
                    .build();

            case MotionSensorEvent e -> MotionSensorAvro.newBuilder()
                    .setLinkQuality(e.getLinkQuality())
                    .setMotion(e.getMotion())
                    .setVoltage(e.getVoltage())
                    .build();

            case SwitchSensorEvent e -> SwitchSensorAvro.newBuilder()
                    .setState(e.getState())
                    .build();

            case TemperatureSensorEvent e -> TemperatureSensorAvro.newBuilder()
                    .setTemperatureC(e.getTemperatureC())
                    .setTemperatureF(e.getTemperatureF())
                    .build();

            default -> throw new IllegalStateException("Unexpected value: " + event);
        };

        return SensorEventAvro.newBuilder()
                .setId(event.getId())
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp())
                .setPayload(payload)
                .build();
    }

    public static HubEventAvro toAvro(HubEvent event) {
        if (event == null) return null;

        Object payload = switch (event) {
            case DeviceAddedEvent e -> DeviceAddedEventAvro.newBuilder()
                    .setId(e.getId())
                    .setType(DeviceTypeAvro.valueOf(e.getDeviceType().name()))
                    .build();

            case DeviceRemovedEvent e -> DeviceRemovedEventAvro.newBuilder()
                    .setId(e.getId())
                    .build();

            case ScenarioAddedEvent e -> ScenarioAddedEventAvro.newBuilder()
                    .setName(e.getName())
                    .setConditions(e.getConditions() == null ? Collections.emptyList() :
                            e.getConditions().stream()
                            .map(EventMapper::mapCondition)
                            .collect(Collectors.toList()))
                    .setActions(e.getActions() == null ? Collections.emptyList() :
                            e.getActions().stream()
                            .map(EventMapper::mapAction)
                            .collect(Collectors.toList()))
                    .build();

            case ScenarioRemovedEvent e -> ScenarioRemovedEventAvro.newBuilder()
                    .setName(e.getName())
                    .build();

            default -> throw new IllegalStateException("Unexpected value: " + event);
        };

        return HubEventAvro.newBuilder()
                .setHubId(event.getHubId())
                .setTimestamp(event.getTimestamp())
                .setPayload(payload)
                .build();
    }

    private static ScenarioConditionAvro mapCondition(ScenarioCondition condition) {
        if (condition == null) return null;

        return ScenarioConditionAvro.newBuilder()
                .setSensorId(condition.getSensorId())
                .setType(ConditionTypeAvro.valueOf(condition.getType().name()))
                .setOperation(ConditionOperationAvro.valueOf(condition.getOperation().name()))
                .setValue(condition.getValue())
                .build();
    }

    private static DeviceActionAvro mapAction(DeviceAction action) {
        if (action == null) return null;

        return DeviceActionAvro.newBuilder()
                .setSensorId(action.getSensorId())
                .setType(ActionTypeAvro.valueOf(action.getType().name()))
                .setValue(action.getValue())
                .build();
    }
}

