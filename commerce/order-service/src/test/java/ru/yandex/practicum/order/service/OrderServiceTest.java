package ru.yandex.practicum.order.service;

import feign.FeignException;
import feign.Request;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import ru.yandex.practicum.order.client.InventoryClient;
import ru.yandex.practicum.order.client.ProductClient;
import ru.yandex.practicum.order.client.dto.InventoryResponse;
import ru.yandex.practicum.order.client.dto.ProductResponse;
import ru.yandex.practicum.order.client.dto.ReleaseRequest;
import ru.yandex.practicum.order.client.dto.ReserveRequest;
import ru.yandex.practicum.order.dto.CreateOrderRequest;
import ru.yandex.practicum.order.dto.OrderDto;
import ru.yandex.practicum.order.dto.OrderItemRequest;
import ru.yandex.practicum.order.entity.Order;
import ru.yandex.practicum.order.exception.OrderProcessingException;
import ru.yandex.practicum.order.repository.OrderRepository;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderServiceTest {

    @Mock
    private OrderRepository orderRepository;

    @Mock
    private ProductClient productClient;

    @Mock
    private InventoryClient inventoryClient;

    @InjectMocks
    private OrderService orderService;

    @Test
    void shouldCreateConfirmedOrderAfterProductLookupAndReserve() {
        when(productClient.getProduct(1L))
                .thenReturn(new ProductResponse(1L, "Smart Lamp", null, new BigDecimal("3490.00"), true));
        when(inventoryClient.reserve(any())).thenReturn(new InventoryResponse(true, 10, "Зарезервировано"));
        when(orderRepository.save(any(Order.class))).thenAnswer(invocation -> invocation.getArgument(0));

        OrderDto result = orderService.create(request(
                new OrderItemRequest(1L, 2)));

        assertThat(result.status())
                .as("Заказ должен сохраняться в статусе CONFIRMED")
                .isEqualTo("CONFIRMED");
        assertThat(result.totalPrice())
                .as("Итоговая стоимость должна считаться по цене из product-service")
                .isEqualByComparingTo("6980.00");
        assertThat(result.items())
                .as("В заказе должен сохраняться снимок товарных данных")
                .singleElement()
                .satisfies(item -> {
                    assertThat(item.productId()).isEqualTo(1L);
                    assertThat(item.productName()).isEqualTo("Smart Lamp");
                    assertThat(item.price()).isEqualByComparingTo("3490.00");
                    assertThat(item.quantity()).isEqualTo(2);
                });

        verify(productClient).getProduct(1L);
        verify(inventoryClient).reserve(new ReserveRequest(1L, 2));
        verify(inventoryClient, never()).release(any());
        verify(orderRepository).save(any(Order.class));
    }

    @Test
    void shouldLookupProductOnceAndReserveAggregatedQuantityForDuplicatedProduct() {
        when(productClient.getProduct(1L))
                .thenReturn(new ProductResponse(1L, "Smart Lamp", null, new BigDecimal("100.00"), true));
        when(inventoryClient.reserve(any())).thenReturn(new InventoryResponse(true, 10, "Зарезервировано"));
        when(orderRepository.save(any(Order.class))).thenAnswer(invocation -> invocation.getArgument(0));

        orderService.create(request(
                new OrderItemRequest(1L, 2),
                new OrderItemRequest(1L, 1)));

        verify(productClient, times(1)).getProduct(1L);
        verify(inventoryClient, times(1)).reserve(new ReserveRequest(1L, 3));
    }

    @Test
    void shouldNotReserveOrSaveWhenProductIsNotActive() {
        when(productClient.getProduct(3L))
                .thenReturn(new ProductResponse(3L, "Retired Device", null, new BigDecimal("100.00"), false));

        assertThatThrownBy(() -> orderService.create(request(new OrderItemRequest(3L, 1))))
                .isInstanceOf(OrderProcessingException.class)
                .hasMessageContaining("снят с продажи");

        verify(inventoryClient, never()).reserve(any());
        verify(inventoryClient, never()).release(any());
        verify(orderRepository, never()).save(any(Order.class));
    }

    @Test
    void shouldNotSaveOrderWhenReservationFails() {
        when(productClient.getProduct(9L))
                .thenReturn(new ProductResponse(9L, "Smart Hub", null, new BigDecimal("900.00"), true));
        when(inventoryClient.reserve(new ReserveRequest(9L, 100))).thenThrow(conflict());

        assertThatThrownBy(() -> orderService.create(request(new OrderItemRequest(9L, 100))))
                .isInstanceOf(OrderProcessingException.class)
                .hasMessageContaining("недостаточно");

        verify(orderRepository, never()).save(any(Order.class));
        verify(inventoryClient, never()).release(any());
    }

    @Test
    void shouldReleaseAlreadyCreatedReserveAndNotSaveWhenScenarioFailsAfterReserve() {
        when(productClient.getProduct(1L))
                .thenReturn(new ProductResponse(1L, "Smart Lamp", null, new BigDecimal("100.00"), true));
        when(productClient.getProduct(2L))
                .thenReturn(new ProductResponse(2L, "Smart Plug", null, new BigDecimal("200.00"), true));
        when(inventoryClient.reserve(new ReserveRequest(1L, 2)))
                .thenReturn(new InventoryResponse(true, 10, "Зарезервировано"));
        when(inventoryClient.reserve(new ReserveRequest(2L, 1))).thenThrow(conflict());
        when(inventoryClient.release(any())).thenReturn(new InventoryResponse(true, 10, "Снят резерв"));

        assertThatThrownBy(() -> orderService.create(request(
                new OrderItemRequest(1L, 2),
                new OrderItemRequest(2L, 1))))
                .isInstanceOf(OrderProcessingException.class);

        ArgumentCaptor<ReleaseRequest> releases = ArgumentCaptor.forClass(ReleaseRequest.class);
        verify(inventoryClient).release(releases.capture());
        assertThat(releases.getValue())
                .as("Компенсация должна снимать ранее созданный резерв по первому товару")
                .isEqualTo(new ReleaseRequest(1L, 2));
        verify(orderRepository, never()).save(any(Order.class));
    }

    @Test
    void shouldNotReserveWhenProductNotFound() {
        when(productClient.getProduct(7L)).thenThrow(notFound());

        assertThatThrownBy(() -> orderService.create(request(new OrderItemRequest(7L, 1))))
                .isInstanceOf(OrderProcessingException.class)
                .hasMessageContaining("не найден");

        verify(inventoryClient, never()).reserve(any());
        verify(orderRepository, never()).save(any(Order.class));
    }

    private static CreateOrderRequest request(OrderItemRequest... items) {
        return new CreateOrderRequest("Unit Buyer", "unit-buyer@example.com", List.of(items));
    }

    private static FeignException conflict() {
        return new FeignException.Conflict("409 Conflict", dummyRequest(), new byte[0], Collections.emptyMap());
    }

    private static FeignException notFound() {
        return new FeignException.NotFound("404 Not Found", dummyRequest(), new byte[0], Collections.emptyMap());
    }

    private static Request dummyRequest() {
        return Request.create("POST", "http://localhost/api/mock", Collections.emptyMap(), new byte[0],
                StandardCharsets.UTF_8);
    }
}
