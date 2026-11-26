package ru.yandex.practicum.order.service;

import feign.FeignException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.order.client.InventoryClient;
import ru.yandex.practicum.order.client.ProductClient;
import ru.yandex.practicum.order.client.dto.ProductResponse;
import ru.yandex.practicum.order.client.dto.ReleaseRequest;
import ru.yandex.practicum.order.client.dto.ReserveRequest;
import ru.yandex.practicum.order.dto.CreateOrderRequest;
import ru.yandex.practicum.order.dto.OrderDto;
import ru.yandex.practicum.order.dto.OrderItemRequest;
import ru.yandex.practicum.order.entity.Order;
import ru.yandex.practicum.order.entity.OrderItem;
import ru.yandex.practicum.order.entity.OrderStatus;
import ru.yandex.practicum.order.exception.NotFoundException;
import ru.yandex.practicum.order.exception.OrderProcessingException;
import ru.yandex.practicum.order.mapper.OrderMapper;
import ru.yandex.practicum.order.repository.OrderRepository;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class OrderService {
    private final OrderRepository orderRepository;
    private final ProductClient productClient;
    private final InventoryClient inventoryClient;

    @Transactional(readOnly = true)
    public List<OrderDto> findAll() {
        return orderRepository.findAllByOrderByCreatedAtDesc().stream()
                .map(OrderMapper::toDto)
                .toList();
    }

    @Transactional(readOnly = true)
    public OrderDto findById(Long id) {
        return OrderMapper.toDto(getOrder(id));
    }

    @Transactional(readOnly = true)
    public List<OrderDto> findByCustomerEmail(String email) {
        return orderRepository.findByCustomerEmailIgnoreCase(email).stream()
                .map(OrderMapper::toDto)
                .toList();
    }

    public OrderDto create(CreateOrderRequest request) {
        List<ReservedPosition> reservations = new ArrayList<>();

        try {
            // 1. Суммарное количество по каждому товару: для склада — один резерв на productId
            Map<Long, Integer> quantityByProduct = aggregateQuantities(request.items());

            // 2. Данные товара запрашиваются один раз на productId и проверяются до резервирования
            Map<Long, ProductResponse> products = new HashMap<>();
            for (Long productId : quantityByProduct.keySet()) {
                products.put(productId, fetchActiveProduct(productId));
            }

            // 3. Резервируем остатки с учётом суммарных количеств
            for (Map.Entry<Long, Integer> entry : quantityByProduct.entrySet()) {
                reserve(entry.getKey(), entry.getValue());
                reservations.add(new ReservedPosition(entry.getKey(), entry.getValue()));
            }

            // 4. Сохраняем заказ со снимком товарных данных
            Order order = buildOrder(request, products);
            return OrderMapper.toDto(orderRepository.save(order));
        } catch (RuntimeException e) {
            releaseReservations(reservations, e);
            throw e;
        }
    }

    private Map<Long, Integer> aggregateQuantities(List<OrderItemRequest> items) {
        Map<Long, Integer> quantityByProduct = new LinkedHashMap<>();
        for (OrderItemRequest item : items) {
            quantityByProduct.merge(item.productId(), item.quantity(), Integer::sum);
        }
        return quantityByProduct;
    }

    private ProductResponse fetchActiveProduct(Long productId) {
        ProductResponse product;
        try {
            product = productClient.getProduct(productId);
        } catch (FeignException.NotFound e) {
            log.warn("product-service: товар id={} не найден", productId);
            throw new OrderProcessingException("Товар с id=" + productId + " не найден");
        } catch (FeignException e) {
            log.error("product-service: ошибка получения товара id={}, статус {}", productId, e.status(), e);
            throw new OrderProcessingException(
                    "Не удалось получить данные товара с id=" + productId + ". Повторите попытку позже", e);
        }

        if (product == null || product.id() == null) {
            throw new OrderProcessingException("Товар с id=" + productId + " не найден");
        }
        if (!Boolean.TRUE.equals(product.active())) {
            throw new OrderProcessingException("Товар с id=" + productId + " снят с продажи");
        }
        return product;
    }

    private void reserve(Long productId, int quantity) {
        try {
            inventoryClient.reserve(new ReserveRequest(productId, quantity));
            log.debug("Зарезервировано {} шт. товара id={}", quantity, productId);
        } catch (FeignException.NotFound e) {
            log.warn("inventory-service: складская запись для товара id={} не найдена", productId);
            throw new OrderProcessingException(
                    "Складская запись для товара с id=" + productId + " не найдена");
        } catch (FeignException.Conflict e) {
            log.warn("inventory-service: недостаточно товара id={} для резерва {} шт.", productId, quantity);
            throw new OrderProcessingException(
                    "Товара с id=" + productId + " недостаточно на складе");
        } catch (FeignException e) {
            log.error("inventory-service: ошибка резервирования товара id={}, статус {}",
                    productId, e.status(), e);
            throw new OrderProcessingException(
                    "Не удалось зарезервировать товар с id=" + productId + ". Повторите попытку позже", e);
        }
    }

    private void releaseReservations(List<ReservedPosition> reservations, RuntimeException cause) {
        for (int i = reservations.size() - 1; i >= 0; i--) {
            ReservedPosition position = reservations.get(i);
            try {
                inventoryClient.release(new ReleaseRequest(position.productId(), position.quantity()));
                log.info("Компенсация: снят резерв {} шт. товара id={}",
                        position.quantity(), position.productId());
            } catch (RuntimeException releaseError) {
                log.error("Компенсация: не удалось снять резерв {} шт. товара id={}",
                        position.quantity(), position.productId(), releaseError);
                cause.addSuppressed(releaseError);
            }
        }
    }

    private Order buildOrder(CreateOrderRequest request, Map<Long, ProductResponse> products) {
        Order order = Order.builder()
                .customerName(request.customerName())
                .customerEmail(request.customerEmail())
                .status(OrderStatus.CONFIRMED)
                .createdAt(LocalDateTime.now())
                .items(new ArrayList<>())
                .build();

        for (OrderItemRequest itemRequest : request.items()) {
            ProductResponse product = products.get(itemRequest.productId());
            OrderItem item = OrderItem.builder()
                    .order(order)
                    .productId(itemRequest.productId())
                    .productName(product.name())
                    .quantity(itemRequest.quantity())
                    .price(product.price())
                    .build();
            order.getItems().add(item);
        }

        order.setTotalPrice(calculateTotalPrice(order.getItems()));
        return order;
    }

    private BigDecimal calculateTotalPrice(List<OrderItem> items) {
        return items.stream()
                .map(item -> item.getPrice() == null
                        ? BigDecimal.ZERO
                        : item.getPrice().multiply(BigDecimal.valueOf(item.getQuantity())))
                .reduce(BigDecimal.ZERO, BigDecimal::add);
    }

    private Order getOrder(Long id) {
        return orderRepository.findById(id)
                .orElseThrow(() -> new NotFoundException("Заказ с id=" + id + " не найден"));
    }

    private record ReservedPosition(Long productId, int quantity) {
    }
}
