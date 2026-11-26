package ru.yandex.practicum.inventory.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.inventory.dto.InventoryDto;
import ru.yandex.practicum.inventory.dto.ReleaseRequest;
import ru.yandex.practicum.inventory.dto.ReserveRequest;
import ru.yandex.practicum.inventory.dto.ReserveResponse;
import ru.yandex.practicum.inventory.dto.UpdateInventoryRequest;
import ru.yandex.practicum.inventory.entity.InventoryRecord;
import ru.yandex.practicum.inventory.exception.InsufficientStockException;
import ru.yandex.practicum.inventory.exception.InvalidReleaseException;
import ru.yandex.practicum.inventory.exception.InventoryConflictException;
import ru.yandex.practicum.inventory.exception.NotFoundException;
import ru.yandex.practicum.inventory.mapper.InventoryMapper;
import ru.yandex.practicum.inventory.repository.InventoryRepository;

import java.util.List;

@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class InventoryService {
    private final InventoryRepository inventoryRepository;

    public List<InventoryDto> findAll() {
        return inventoryRepository.findAll().stream()
                .map(InventoryMapper::toDto)
                .toList();
    }

    public InventoryDto findByProductId(Long productId) {
        return InventoryMapper.toDto(getRecord(productId));
    }

    @Transactional
    public InventoryDto create(UpdateInventoryRequest request) {
        if (inventoryRepository.existsByProductId(request.productId())) {
            throw new InventoryConflictException(
                    "Складская запись для товара id=" + request.productId() + " уже существует");
        }

        InventoryRecord record = InventoryRecord.builder()
                .productId(request.productId())
                .quantity(request.quantity())
                .reservedQuantity(0)
                .build();

        return InventoryMapper.toDto(inventoryRepository.save(record));
    }

    @Transactional
    public InventoryDto update(UpdateInventoryRequest request) {
        InventoryRecord record = getRecord(request.productId());

        if (request.quantity() < record.getReservedQuantity()) {
            throw new InventoryConflictException(
                    "Общее количество " + request.quantity()
                            + " меньше зарезервированного " + record.getReservedQuantity()
                            + " для товара id=" + request.productId());
        }

        record.setQuantity(request.quantity());
        return InventoryMapper.toDto(inventoryRepository.save(record));
    }

    @Transactional
    public ReserveResponse reserve(ReserveRequest request) {
        InventoryRecord record = getRecord(request.productId());
        int available = InventoryMapper.available(record);

        if (available < request.quantity()) {
            throw new InsufficientStockException(
                    "Недостаточно товара id=" + request.productId()
                            + ": доступно " + available + ", запрошено " + request.quantity());
        }

        record.setReservedQuantity(record.getReservedQuantity() + request.quantity());
        InventoryRecord saved = inventoryRepository.save(record);

        return new ReserveResponse(
                true,
                InventoryMapper.available(saved),
                "Зарезервировано " + request.quantity() + " шт. товара id=" + request.productId());
    }

    @Transactional
    public ReserveResponse release(ReleaseRequest request) {
        InventoryRecord record = getRecord(request.productId());

        if (record.getReservedQuantity() < request.quantity()) {
            throw new InvalidReleaseException(
                    "Невозможно снять резерв " + request.quantity()
                            + " шт. для товара id=" + request.productId()
                            + ": сейчас зарезервировано " + record.getReservedQuantity());
        }

        record.setReservedQuantity(record.getReservedQuantity() - request.quantity());
        InventoryRecord saved = inventoryRepository.save(record);

        return new ReserveResponse(
                true,
                InventoryMapper.available(saved),
                "Снят резерв " + request.quantity() + " шт. товара id=" + request.productId());
    }

    private InventoryRecord getRecord(Long productId) {
        return inventoryRepository.findByProductId(productId)
                .orElseThrow(() -> new NotFoundException(
                        "Складская запись для товара id=" + productId + " не найдена"));
    }
}
