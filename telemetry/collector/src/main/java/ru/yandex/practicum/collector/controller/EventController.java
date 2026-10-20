package ru.yandex.practicum.collector.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.yandex.practicum.collector.dto.hubs.HubEvent;
import ru.yandex.practicum.collector.dto.sensors.SensorEvent;
import ru.yandex.practicum.collector.service.EventService;

@RestController
@RequestMapping("/events")
@RequiredArgsConstructor
@Validated
public class EventController {
    private final EventService eventService;

    @PostMapping("/sensors")
    public void collectSensorEvent(@Valid @RequestBody final SensorEvent sensorEvent) {
        eventService.collectSensorEvent(sensorEvent);
    }

    @PostMapping("/hubs")
    public void collectHubEvent(@Valid @RequestBody final HubEvent hubEvent) {
        eventService.collectHubEvent(hubEvent);
    }
}
