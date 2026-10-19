package ru.yandex.practicum.collector.service;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
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

    @Value("${spring.kafka.topics.sensors}")
    private final String SENSORS_TOPIC;

    @Value("${spring.kafka.topics.hubs}")
    private final String HUBS_TOPIC;

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
