package ru.yandex.practicum.collector.mapper;

import com.google.protobuf.Timestamp;
import ru.yandex.practicum.collector.dto.sensors.*;
import ru.yandex.practicum.grpc.telemetry.event.DeviceActionProto;
import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.ScenarioConditionProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;
import ru.yandex.practicum.kafka.telemetry.event.*;

import java.time.Instant;
import java.util.stream.Collectors;

public class EventMapper {

    public static SensorEventAvro toAvro(SensorEventProto proto) {
        if (proto == null) return null;

        Object payload = switch (proto.getPayloadCase()) {
            case CLIMATE_SENSOR -> ClimateSensorAvro.newBuilder()
                    .setTemperatureC(proto.getClimateSensor().getTemperatureC())
                    .setHumidity(proto.getClimateSensor().getHumidity())
                    .setCo2Level(proto.getClimateSensor().getCo2Level())
                    .build();

            case LIGHT_SENSOR -> LightSensorAvro.newBuilder()
                    .setLinkQuality(proto.getLightSensor().getLinkQuality())
                    .setLuminosity(proto.getLightSensor().getLuminosity())
                    .build();

            case MOTION_SENSOR -> MotionSensorAvro.newBuilder()
                    .setLinkQuality(proto.getMotionSensor().getLinkQuality())
                    .setMotion(proto.getMotionSensor().getMotion())
                    .setVoltage(proto.getMotionSensor().getVoltage())
                    .build();

            case SWITCH_SENSOR -> SwitchSensorAvro.newBuilder()
                    .setState(proto.getSwitchSensor().getState())
                    .build();

            case TEMPERATURE_SENSOR -> TemperatureSensorAvro.newBuilder()
                    .setTemperatureC(proto.getTemperatureSensor().getTemperatureC())
                    .setTemperatureF(proto.getTemperatureSensor().getTemperatureF())
                    .build();

            default -> throw new IllegalStateException("Unexpected proto payload case: " + proto.getPayloadCase());
        };

        return SensorEventAvro.newBuilder()
                .setId(proto.getId())
                .setHubId(proto.getHubId())
                .setTimestamp(mapTimestamp(proto.getTimestamp()))
                .setPayload(payload)
                .build();
    }

    public static HubEventAvro toAvro(HubEventProto proto) {
        if (proto == null) return null;

        Object payload = switch (proto.getPayloadCase()) {
            case DEVICE_ADDED -> DeviceAddedEventAvro.newBuilder()
                    .setId(proto.getDeviceAdded().getId())
                    .setType(DeviceTypeAvro.valueOf(proto.getDeviceAdded().getType().name()))
                    .build();

            case DEVICE_REMOVED -> DeviceRemovedEventAvro.newBuilder()
                    .setId(proto.getDeviceRemoved().getId())
                    .build();

            case SCENARIO_ADDED -> ScenarioAddedEventAvro.newBuilder()
                    .setName(proto.getScenarioAdded().getName())
                    .setConditions(proto.getScenarioAdded().getConditionList().stream()
                            .map(EventMapper::mapCondition)
                            .collect(Collectors.toList()))
                    .setActions(proto.getScenarioAdded().getActionList().stream()
                            .map(EventMapper::mapAction)
                            .collect(Collectors.toList()))
                    .build();

            case SCENARIO_REMOVED -> ScenarioRemovedEventAvro.newBuilder()
                    .setName(proto.getScenarioRemoved().getName())
                    .build();

            default -> throw new IllegalStateException("Unexpected proto payload case: " + proto.getPayloadCase());
        };

        return HubEventAvro.newBuilder()
                .setHubId(proto.getHubId())
                .setTimestamp(mapTimestamp(proto.getTimestamp()))
                .setPayload(payload)
                .build();
    }

    private static Instant mapTimestamp(Timestamp protoTimestamp) {
        if (protoTimestamp == null) return Instant.now();
        return Instant.ofEpochSecond(protoTimestamp.getSeconds(), protoTimestamp.getNanos());
    }

    private static ScenarioConditionAvro mapCondition(ScenarioConditionProto condition) {
        if (condition == null) return null;

        ScenarioConditionAvro.Builder builder = ScenarioConditionAvro.newBuilder()
                .setSensorId(condition.getSensorId())
                .setType(ConditionTypeAvro.valueOf(condition.getType().name()))
                .setOperation(ConditionOperationAvro.valueOf(condition.getOperation().name()));

        switch (condition.getValueCase()) {
            case INT_VALUE:
                builder.setValue(condition.getIntValue());
                break;
            case BOOL_VALUE:
                builder.setValue(condition.getBoolValue());
                break;
            default:
                builder.setValue(null);
        }

        return builder.build();
    }

    private static DeviceActionAvro mapAction(DeviceActionProto action) {
        if (action == null) return null;

        DeviceActionAvro.Builder builder = DeviceActionAvro.newBuilder()
                .setSensorId(action.getSensorId())
                .setType(ActionTypeAvro.valueOf(action.getType().name()));

        if (action.hasValue()) {
            builder.setValue(action.getValue());
        } else {
            builder.setValue(null);
        }

        return builder.build();
    }

//    public static SensorEventAvro toAvro(SensorEvent event) {
//        if (event == null) return null;
//
//        Object payload = switch (event) {
//            case ClimateSensorEvent e -> ClimateSensorAvro.newBuilder()
//                    .setTemperatureC(e.getTemperatureC())
//                    .setHumidity(e.getHumidity())
//                    .setCo2Level(e.getCo2Level())
//                    .build();
//
//            case LightSensorEvent e -> LightSensorAvro.newBuilder()
//                    .setLinkQuality(e.getLinkQuality())
//                    .setLuminosity(e.getLuminosity())
//                    .build();
//
//            case MotionSensorEvent e -> MotionSensorAvro.newBuilder()
//                    .setLinkQuality(e.getLinkQuality())
//                    .setMotion(e.getMotion())
//                    .setVoltage(e.getVoltage())
//                    .build();
//
//            case SwitchSensorEvent e -> SwitchSensorAvro.newBuilder()
//                    .setState(e.getState())
//                    .build();
//
//            case TemperatureSensorEvent e -> TemperatureSensorAvro.newBuilder()
//                    .setTemperatureC(e.getTemperatureC())
//                    .setTemperatureF(e.getTemperatureF())
//                    .build();
//
//            default -> throw new IllegalStateException("Unexpected value: " + event);
//        };
//
//        return SensorEventAvro.newBuilder()
//                .setId(event.getId())
//                .setHubId(event.getHubId())
//                .setTimestamp(event.getTimestamp())
//                .setPayload(payload)
//                .build();
//    }
//
//    public static HubEventAvro toAvro(HubEvent event) {
//        if (event == null) return null;
//
//        Object payload = switch (event) {
//            case DeviceAddedEvent e -> DeviceAddedEventAvro.newBuilder()
//                    .setId(e.getId())
//                    .setType(DeviceTypeAvro.valueOf(e.getDeviceType().name()))
//                    .build();
//
//            case DeviceRemovedEvent e -> DeviceRemovedEventAvro.newBuilder()
//                    .setId(e.getId())
//                    .build();
//
//            case ScenarioAddedEvent e -> ScenarioAddedEventAvro.newBuilder()
//                    .setName(e.getName())
//                    .setConditions(e.getConditions() == null ? Collections.emptyList() :
//                            e.getConditions().stream()
//                            .map(EventMapper::mapCondition)
//                            .collect(Collectors.toList()))
//                    .setActions(e.getActions() == null ? Collections.emptyList() :
//                            e.getActions().stream()
//                            .map(EventMapper::mapAction)
//                            .collect(Collectors.toList()))
//                    .build();
//
//            case ScenarioRemovedEvent e -> ScenarioRemovedEventAvro.newBuilder()
//                    .setName(e.getName())
//                    .build();
//
//            default -> throw new IllegalStateException("Unexpected value: " + event);
//        };
//
//        return HubEventAvro.newBuilder()
//                .setHubId(event.getHubId())
//                .setTimestamp(event.getTimestamp())
//                .setPayload(payload)
//                .build();
//    }
//
//    private static ScenarioConditionAvro mapCondition(ScenarioCondition condition) {
//        if (condition == null) return null;
//
//        return ScenarioConditionAvro.newBuilder()
//                .setSensorId(condition.getSensorId())
//                .setType(ConditionTypeAvro.valueOf(condition.getType().name()))
//                .setOperation(ConditionOperationAvro.valueOf(condition.getOperation().name()))
//                .setValue(condition.getValue())
//                .build();
//    }
//
//    private static DeviceActionAvro mapAction(DeviceAction action) {
//        if (action == null) return null;
//
//        return DeviceActionAvro.newBuilder()
//                .setSensorId(action.getSensorId())
//                .setType(ActionTypeAvro.valueOf(action.getType().name()))
//                .setValue(action.getValue())
//                .build();
//    }
}

