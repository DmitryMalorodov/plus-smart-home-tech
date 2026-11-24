package ru.yandex.practicum.product.service;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.yandex.practicum.product.dto.CreateProductRequest;
import ru.yandex.practicum.product.dto.ProductDto;
import ru.yandex.practicum.product.dto.UpdateProductRequest;
import ru.yandex.practicum.product.entity.Category;
import ru.yandex.practicum.product.entity.Product;
import ru.yandex.practicum.product.exception.NotFoundException;
import ru.yandex.practicum.product.mapper.CatalogMapper;
import ru.yandex.practicum.product.repository.CategoryRepository;
import ru.yandex.practicum.product.repository.ProductRepository;

import java.util.List;

@Service
@RequiredArgsConstructor
@Transactional(readOnly = true)
public class ProductService {
    private final ProductRepository productRepository;
    private final CategoryRepository categoryRepository;

    public List<ProductDto> findAllActive() {
        return productRepository.findAllByActiveTrue().stream()
                .map(CatalogMapper::toDto)
                .toList();
    }

    public List<ProductDto> findByCategory(Long categoryId) {
        return productRepository.findAllByActiveTrueAndCategoryId(categoryId).stream()
                .map(CatalogMapper::toDto)
                .toList();
    }

    public List<ProductDto> search(String query) {
        if (query == null || query.isBlank()) {
            return List.of();
        }
        return productRepository.findAllByActiveTrueAndNameContainingIgnoreCase(query.trim()).stream()
                .map(CatalogMapper::toDto)
                .toList();
    }

    public ProductDto findById(Long id) {
        return CatalogMapper.toDto(getProduct(id));
    }

    @Transactional
    public ProductDto create(CreateProductRequest request) {
        Product product = Product.builder()
                .name(request.name())
                .description(request.description())
                .price(request.price())
                .category(resolveCategory(request.categoryId()))
                .imageUrl(request.imageUrl())
                .active(true)
                .build();
        return CatalogMapper.toDto(productRepository.save(product));
    }

    @Transactional
    public ProductDto update(Long id, UpdateProductRequest request) {
        Product product = getProduct(id);

        if (request.name() != null) product.setName(request.name());
        if (request.description() != null) product.setDescription(request.description());
        if (request.price() != null) product.setPrice(request.price());
        if (request.categoryId() != null) product.setCategory(resolveCategory(request.categoryId()));
        if (request.imageUrl() != null) product.setImageUrl(request.imageUrl());
        if (request.active() != null) product.setActive(request.active());

        return CatalogMapper.toDto(productRepository.save(product));
    }

    private Product getProduct(Long id) {
        return productRepository.findById(id)
                .orElseThrow(() -> new NotFoundException("Товар с id=" + id + " не найден"));
    }

    private Category resolveCategory(Long categoryId) {
        if (categoryId == null) return null;

        return categoryRepository.findById(categoryId)
                .orElseThrow(() -> new NotFoundException("Категория с id=" + categoryId + " не найдена"));
    }
}
