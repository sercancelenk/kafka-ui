package com.trendyol.kafka.stream.api.infra;

import jakarta.annotation.PostConstruct;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Map;

@Data
@Component
@ConfigurationProperties(prefix = "app")
@Slf4j
public class AppConfig {
    private MaxThreads maxThreads;
    private CacheConfig cache;

    @PostConstruct
    public void init() {
        log.info("Application configs: {}", this);
    }

    public record MaxThreads(Integer consumerGroupByTopic) {
    }

    public record CacheConfig(Map<String, String> timeouts) {
    }
}
