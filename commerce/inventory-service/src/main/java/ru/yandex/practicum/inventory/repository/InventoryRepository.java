package ru.yandex.practicum.inventory.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.yandex.practicum.inventory.entity.InventoryRecord;

import java.util.Optional;

public interface InventoryRepository extends JpaRepository<InventoryRecord, Long> {

    Optional<InventoryRecord> findByProductId(Long productId);

    boolean existsByProductId(Long productId);
}
