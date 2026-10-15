package ru.yandex.practicum.collector.dto.hubs;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import ru.yandex.practicum.collector.dto.hubs.device.DeviceAddedEvent;
import ru.yandex.practicum.collector.dto.hubs.device.DeviceRemovedEvent;
import ru.yandex.practicum.collector.dto.hubs.scenario.ScenarioAddedEvent;
import ru.yandex.practicum.collector.dto.hubs.scenario.ScenarioRemovedEvent;

import java.time.Instant;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.EXISTING_PROPERTY,
        property = "type",
        defaultImpl = HubEventType.class
)
@JsonSubTypes({
        @JsonSubTypes.Type(value = DeviceAddedEvent.class, name = "DEVICE_ADDED"),
        @JsonSubTypes.Type(value = DeviceRemovedEvent.class, name = "DEVICE_REMOVED"),
        @JsonSubTypes.Type(value = ScenarioAddedEvent.class, name = "SCENARIO_ADDED"),
        @JsonSubTypes.Type(value = ScenarioRemovedEvent.class, name = "SCENARIO_REMOVED")
})
@Getter
@Setter
@ToString
public abstract class HubEvent {
    @NotBlank(message = "Идентификатор хаба не может быть пустым")
    private String hubId;
    private Instant timestamp = Instant.now();

    @NotNull(message = "Тип события не может быть пустым")
    public abstract HubEventType getType();
}
