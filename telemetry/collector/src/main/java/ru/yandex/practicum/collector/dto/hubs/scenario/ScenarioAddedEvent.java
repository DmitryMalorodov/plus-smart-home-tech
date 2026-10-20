package ru.yandex.practicum.collector.dto.hubs.scenario;

import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import ru.yandex.practicum.collector.dto.hubs.HubEvent;
import ru.yandex.practicum.collector.dto.hubs.HubEventType;

import java.util.List;

@Getter
@Setter
@ToString(callSuper = true)
public class ScenarioAddedEvent extends HubEvent {
    @Size(min = 3, message = "Название добавленного сценария должно содержать не менее 3 символов.")
    private String name;

    @NotEmpty(message = "Список условий не может быть пустым.")
    private List<ScenarioCondition> conditions;

    @NotEmpty(message = "Список действий не может быть пустым.")
    private List<DeviceAction> actions;

    @Override
    public HubEventType getType() {
        return HubEventType.SCENARIO_ADDED;
    }
}
