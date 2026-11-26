package ru.yandex.practicum.order.client.dto;

public record ReserveRequest(
        Long productId,
        Integer quantity
) {
}
