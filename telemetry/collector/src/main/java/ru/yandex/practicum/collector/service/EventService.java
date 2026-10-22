package ru.yandex.practicum.collector.service;

import ru.yandex.practicum.grpc.telemetry.event.HubEventProto;
import ru.yandex.practicum.grpc.telemetry.event.SensorEventProto;

public interface EventService {
    //void collectSensorEvent(SensorEvent event);
    //void collectHubEvent(HubEvent event);

    void collectSensorEvent(SensorEventProto protoEvent);
    void collectHubEvent(HubEventProto protoEvent);
}
