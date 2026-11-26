package ru.yandex.practicum.order.client.dto;

public record ReleaseRequest(
        Long productId,
        Integer quantity
) {
}
