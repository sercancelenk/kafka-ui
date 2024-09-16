package com.trendyol.kafka.stream.api.adapters.rest.filter.interceptor;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.MDC;
import org.springframework.web.servlet.HandlerInterceptor;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;

import static com.trendyol.kafka.stream.api.domain.AuditorConsts.*;


@Slf4j
public class RequestInfoInterceptor implements HandlerInterceptor {

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        String remoteHost = request.getRemoteHost();
        String userAgent = getHeaderValue(request, "User-Agent");
        String queryString = request.getQueryString();
        String projectId = getHeaderValue(request, X_PROJECT_ID.getHeaderKey());
        String clusterId = getHeaderValue(request, X_CLUSTER_ID.getHeaderKey());
        AtomicReference<String> requestPath = new AtomicReference<>(request.getRequestURI());
        Optional.ofNullable(queryString)
                .ifPresent(q -> requestPath.set(requestPath + "?" + q));

        String correlationId = getCorrelationId(request);

        MDC.put(X_CORRELATION_ID.getHeaderKey(), correlationId);
        MDC.put(X_REMOTE_HOST.getHeaderKey(), remoteHost);
        MDC.put(X_REQUEST_PATH.getHeaderKey(), requestPath.get());
        MDC.put(X_USER_AGENT.getHeaderKey(), userAgent);
        MDC.put(X_PROJECT_ID.getHeaderKey(), projectId);
        MDC.put(X_CLUSTER_ID.getHeaderKey(), clusterId);
        log.info("Incoming request");
        return HandlerInterceptor.super.preHandle(request, response, handler);
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) {
        MDC.clear();
    }

    private String getCorrelationId(HttpServletRequest request) {
        String correlationId = getHeaderValue(request, X_CORRELATION_ID.getHeaderKey());
        return StringUtils.isNotEmpty(correlationId) ? correlationId : UUID.randomUUID().toString();
    }

    private String getHeaderValue(HttpServletRequest request, String headerName) {
        return Optional.ofNullable(request.getHeader(headerName)).orElse("");
    }
}