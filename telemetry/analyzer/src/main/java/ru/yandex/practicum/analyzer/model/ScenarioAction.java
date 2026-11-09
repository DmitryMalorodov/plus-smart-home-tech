package ru.yandex.practicum.analyzer.model;

import jakarta.persistence.*;
import lombok.*;
import java.io.Serializable;

@Entity
@Table(name = "scenario_actions")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class ScenarioAction {
    @EmbeddedId
    private Id id;

    @MapsId("scenarioId")
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "scenario_id", insertable = false, updatable = false)
    private Scenario scenario;

    @MapsId("sensorId")
    @ManyToOne(fetch = FetchType.EAGER)
    @JoinColumn(name = "sensor_id", insertable = false, updatable = false)
    private Sensor sensor;

    @MapsId("actionId")
    @ManyToOne(cascade = CascadeType.ALL, fetch = FetchType.EAGER)
    @JoinColumn(name = "action_id", insertable = false, updatable = false)
    private Action action;

    @Embeddable
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class Id implements Serializable {
        @Column(name = "scenario_id")
        private Long scenarioId;

        @Column(name = "sensor_id")
        private String sensorId;

        @Column(name = "action_id")
        private Long actionId;
    }
}
