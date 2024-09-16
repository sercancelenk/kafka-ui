package com.trendyol.kafka.stream.api.domain;

import lombok.Getter;

public enum AuditorConsts {

    X_CORRELATION_ID("x-correlationid"),
    X_AGENTNAME("x-agentname"),
    X_EXECUTOR_USER("x-executor-user"),
    X_REMOTE_HOST("x-remote-host"),
    X_REQUEST_PATH("x-request-path"),
    X_USER_AGENT("x-user-agent"),

    X_PROJECT_ID("x-project-id"),
    X_CLUSTER_ID("x-cluster-id");

    @Getter
    final String headerKey;

    AuditorConsts(String headerKey) {
        this.headerKey = headerKey;
    }
}