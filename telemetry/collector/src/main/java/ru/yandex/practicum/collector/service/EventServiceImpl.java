package ru.yandex.practicum.collector.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import ru.yandex.practicum.collector.dto.hubs.HubEvent;
import ru.yandex.practicum.collector.dto.sensors.SensorEvent;
import ru.yandex.practicum.collector.kafka.EventKafkaProducer;
import ru.yandex.practicum.collector.mapper.EventMapper;
import ru.yandex.practicum.kafka.telemetry.event.HubEventAvro;
import ru.yandex.practicum.kafka.telemetry.event.SensorEventAvro;

@Service
@RequiredArgsConstructor
public class EventServiceImpl implements EventService {
    private final EventKafkaProducer eventKafkaProducer;
    private static final String SENSORS_TOPIC = "telemetry.sensors.v1";
    private static final String HUBS_TOPIC = "telemetry.hubs.v1";

    @Override
    public void collectSensorEvent(SensorEvent event) {
        SensorEventAvro avroRecord = EventMapper.toAvro(event);
        eventKafkaProducer.send(SENSORS_TOPIC, event.getHubId(), avroRecord);
    }

    @Override
    public void collectHubEvent(HubEvent event) {
        HubEventAvro avroRecord = EventMapper.toAvro(event);
        eventKafkaProducer.send(HUBS_TOPIC, event.getHubId(), avroRecord);
    }
}
