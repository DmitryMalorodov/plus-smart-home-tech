package ru.yandex.practicum.collector.dto.hubs.scenario;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@ToString
public class ScenarioCondition {
    private String sensorId;
    private ConditionType type;
    private Operation operation;
    private Integer value;
}
