package ru.yandex.practicum.order.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import ru.yandex.practicum.order.client.dto.InventoryResponse;
import ru.yandex.practicum.order.client.dto.ReleaseRequest;
import ru.yandex.practicum.order.client.dto.ReserveRequest;

@FeignClient(name = "inventory-service", path = "/api/inventory")
public interface InventoryClient {

    @PostMapping("/reserve")
    InventoryResponse reserve(@RequestBody ReserveRequest request);

    @PostMapping("/release")
    InventoryResponse release(@RequestBody ReleaseRequest request);
}
