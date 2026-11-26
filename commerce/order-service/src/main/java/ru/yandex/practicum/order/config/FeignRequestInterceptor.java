package ru.yandex.practicum.order.config;

import feign.RequestInterceptor;
import feign.RequestTemplate;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import java.util.UUID;

/**
 * Передаёт в соседние сервисы технический заголовок X-Request-Id
 * и признак источника вызова X-Source-Service.
 * Если входящий запрос уже содержит X-Request-Id, он пробрасывается дальше.
 * Иначе создаётся новый идентификатор
 */
@Slf4j
@Component
public class FeignRequestInterceptor implements RequestInterceptor {
    public static final String REQUEST_ID_HEADER = "X-Request-Id";
    public static final String SOURCE_SERVICE_HEADER = "X-Source-Service";

    private static final String SOURCE_SERVICE = "order-service";
    private static final String REQUEST_ID_ATTRIBUTE = FeignRequestInterceptor.class.getName() + ".REQUEST_ID";

    @Override
    public void apply(RequestTemplate template) {
        String requestId = resolveRequestId();
        template.header(REQUEST_ID_HEADER, requestId);
        template.header(SOURCE_SERVICE_HEADER, SOURCE_SERVICE);
        log.debug("Feign-вызов {} {} с заголовком {}={}",
                template.method(), template.url(), REQUEST_ID_HEADER, requestId);
    }

    private String resolveRequestId() {
        if (RequestContextHolder.getRequestAttributes() instanceof ServletRequestAttributes attributes) {
            HttpServletRequest request = attributes.getRequest();

            String incomingRequestId = request.getHeader(REQUEST_ID_HEADER);
            if (StringUtils.hasText(incomingRequestId)) {
                return incomingRequestId;
            }

            Object existing = attributes.getAttribute(REQUEST_ID_ATTRIBUTE, RequestAttributes.SCOPE_REQUEST);
            if (existing instanceof String requestId) {
                return requestId;
            }

            String requestId = UUID.randomUUID().toString();
            attributes.setAttribute(REQUEST_ID_ATTRIBUTE, requestId, RequestAttributes.SCOPE_REQUEST);
            return requestId;
        }

        return UUID.randomUUID().toString();
    }
}
