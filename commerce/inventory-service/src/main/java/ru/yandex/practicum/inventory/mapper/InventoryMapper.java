package ru.yandex.practicum.inventory.mapper;

import ru.yandex.practicum.inventory.dto.InventoryDto;
import ru.yandex.practicum.inventory.entity.InventoryRecord;

public final class InventoryMapper {

    private InventoryMapper() {
    }

    public static InventoryDto toDto(InventoryRecord record) {
        if (record == null) return null;

        return new InventoryDto(
                record.getId(),
                record.getProductId(),
                record.getQuantity(),
                record.getReservedQuantity(),
                available(record)
        );
    }

    public static int available(InventoryRecord record) {
        return record.getQuantity() - record.getReservedQuantity();
    }
}
