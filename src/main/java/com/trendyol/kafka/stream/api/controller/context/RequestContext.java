package com.trendyol.kafka.stream.api.controller.context;

import com.trendyol.kafka.stream.api.model.Exceptions;

import java.util.Map;
import java.util.Optional;

public class RequestContext {

    private static final ThreadLocal<Map<String, String>> headers = new ThreadLocal<>();

    // Set headers in ThreadLocal
    public static void setHeaders(Map<String, String> requestHeaders) {
        headers.set(requestHeaders);
    }

    // Get headers from ThreadLocal
    public static Map<String, String> getHeaders() {
        return headers.get();
    }

    // Get a specific header by name
    public static String getHeader(String headerName) {
        Map<String, String> requestHeaders = headers.get();
        if (requestHeaders != null) {
            return requestHeaders.get(headerName);
        }
        return null;
    }

    public static String getProjectId() {
        return Optional.ofNullable(getHeader("x-project-id"))
                .orElseThrow(() -> Exceptions.MissingHeaderException.builder().params(new String[]{"x-project-id"}).build());
    }

    public static String getClusterId() {
        return Optional.ofNullable(getHeader("x-cluster-id"))
                .orElseThrow(() -> Exceptions.MissingHeaderException.builder().params(new String[]{"x-cluser-id"}).build());

    }

    // Clear the headers after request is done (important for memory management)
    public static void clear() {
        headers.remove();
    }
}