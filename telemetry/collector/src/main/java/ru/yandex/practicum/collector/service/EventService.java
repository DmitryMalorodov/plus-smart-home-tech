package ru.yandex.practicum.collector.service;

import ru.yandex.practicum.collector.dto.hubs.HubEvent;
import ru.yandex.practicum.collector.dto.sensors.SensorEvent;

public interface EventService {
    void collectSensorEvent(SensorEvent event);
    void collectHubEvent(HubEvent event);
}
