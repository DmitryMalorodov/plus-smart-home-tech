package ru.yandex.practicum.collector.controller;

import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import lombok.RequiredArgsConstructor;
import net.devh.boot.grpc.server.service.GrpcService;

import ru.yandex.practicum.collector.service.EventService;
import ru.yandex.practicum.grpc.telemetry.collector.CollectorControllerGrpc;
import ru.yandex.practicum.grpc.telemetry.event.*;

@RequiredArgsConstructor
@GrpcService
public class EventController extends CollectorControllerGrpc.CollectorControllerImplBase {
    private final EventService eventService;

    @Override
    public void collectSensorEvent(SensorEventProto request, StreamObserver<Empty> responseObserver) {
        try {
            eventService.collectSensorEvent(request);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(new StatusRuntimeException(
                    Status.INTERNAL
                            .withDescription(e.getLocalizedMessage())
                            .withCause(e)
            ));
        }
    }

    @Override
    public void collectHubEvent(HubEventProto request, StreamObserver<Empty> responseObserver) {
        try {
            eventService.collectHubEvent(request);
            responseObserver.onNext(Empty.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception e) {
            responseObserver.onError(Status.INTERNAL
                    .withDescription(e.getLocalizedMessage())
                    .asRuntimeException());
        }
    }
//
//    @PostMapping("/sensors")
//    public void collectSensorEvent(@Valid @RequestBody final SensorEvent sensorEvent) {
//        eventService.collectSensorEvent(sensorEvent);
//    }
//
//    @PostMapping("/hubs")
//    public void collectHubEvent(@Valid @RequestBody final HubEvent hubEvent) {
//        eventService.collectHubEvent(hubEvent);
//    }
}
