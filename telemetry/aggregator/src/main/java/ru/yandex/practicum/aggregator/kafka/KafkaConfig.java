package ru.yandex.practicum.aggregator.kafka;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

@Configuration
public class KafkaConfig {
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.consumer.group-id}")
    private String consumerGroupId;
    @Value("${spring.kafka.consumer.key-deserializer}")
    private String consumerKeyDeserializer;
    @Value("${spring.kafka.consumer.value-deserializer}")
    private String consumerValueDeserializer;

    @Value("${spring.kafka.producer.key-serializer}")
    private String producerKeySerializer;
    @Value("${spring.kafka.producer.value-serializer}")
    private String producerValueSerializer;

    @Bean
    @Scope("prototype")
    public EventKafkaConsumer eventKafkaConsumer() {
        return new EventKafkaConsumer(
                bootstrapServers,
                consumerGroupId,
                consumerKeyDeserializer,
                consumerValueDeserializer
        );
    }

    @Bean
    @Scope("prototype")
    public EventKafkaProducer eventKafkaProducer() {
        return new EventKafkaProducer(
                bootstrapServers,
                producerKeySerializer,
                producerValueSerializer
        );
    }
}
