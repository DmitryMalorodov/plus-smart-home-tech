package ru.yandex.practicum.order.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.order.dto.CreateOrderRequest;
import ru.yandex.practicum.order.dto.OrderDto;
import ru.yandex.practicum.order.dto.OrderItemRequest;
import ru.yandex.practicum.order.entity.Order;
import ru.yandex.practicum.order.entity.OrderItem;
import ru.yandex.practicum.order.entity.OrderStatus;
import ru.yandex.practicum.order.exception.NotFoundException;
import ru.yandex.practicum.order.mapper.OrderMapper;
import ru.yandex.practicum.order.repository.OrderRepository;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class OrderService {
    private final OrderRepository orderRepository;

    public List<OrderDto> findAll() {
        return orderRepository.findAllByOrderByCreatedAtDesc().stream()
                .map(OrderMapper::toDto)
                .toList();
    }

    public OrderDto findById(Long id) {
        return OrderMapper.toDto(getOrder(id));
    }

    public List<OrderDto> findByCustomerEmail(String email) {
        return orderRepository.findByCustomerEmailIgnoreCase(email).stream()
                .map(OrderMapper::toDto)
                .toList();
    }

    @Transactional
    public OrderDto create(CreateOrderRequest request) {
        Order order = Order.builder()
                .customerName(request.customerName())
                .customerEmail(request.customerEmail())
                .status(OrderStatus.CREATED)
                .createdAt(LocalDateTime.now())
                .items(new ArrayList<>())
                .build();

        for (OrderItemRequest itemRequest : request.items()) {
            OrderItem item = OrderItem.builder()
                    .order(order)
                    .productId(itemRequest.productId())
                    .productName(itemRequest.productName())
                    .quantity(itemRequest.quantity())
                    .price(itemRequest.price())
                    .build();
            order.getItems().add(item);
        }

        order.setTotalPrice(calculateTotalPrice(order.getItems()));

        return OrderMapper.toDto(orderRepository.save(order));
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
}
