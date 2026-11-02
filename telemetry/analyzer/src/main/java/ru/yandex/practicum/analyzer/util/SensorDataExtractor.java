package ru.yandex.practicum.analyzer.util;

import ru.yandex.practicum.analyzer.model.ConditionType;
import ru.yandex.practicum.kafka.telemetry.event.*;

public class SensorDataExtractor {

    public static Integer extractValue(Object data, ConditionType type) {
        if (data == null) return null;

        return switch (type) {
            case TEMPERATURE -> {
                if (data instanceof ClimateSensorAvro c) yield c.getTemperatureC();
                if (data instanceof TemperatureSensorAvro t) yield t.getTemperatureC();
                yield null;
            }
            case HUMIDITY -> (data instanceof ClimateSensorAvro c) ? c.getHumidity() : null;
            case CO2LEVEL -> (data instanceof ClimateSensorAvro c) ? c.getCo2Level() : null;
            case LUMINOSITY -> (data instanceof LightSensorAvro l) ? l.getLuminosity() : null;
            case MOTION -> (data instanceof MotionSensorAvro m) ? (m.getMotion() ? 1 : 0) : null;
            case SWITCH -> (data instanceof SwitchSensorAvro s) ? (s.getState() ? 1 : 0) : null;
        };
    }
}
