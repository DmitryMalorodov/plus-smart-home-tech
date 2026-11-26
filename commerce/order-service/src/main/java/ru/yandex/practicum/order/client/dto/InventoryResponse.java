package ru.yandex.practicum.order.client.dto;

public record InventoryResponse(
        boolean success,
        Integer availableQuantity,
        String message
) {
}
