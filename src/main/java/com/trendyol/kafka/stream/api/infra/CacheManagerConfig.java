package com.trendyol.kafka.stream.api.infra;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import io.swagger.v3.oas.annotations.Operation;
import lombok.RequiredArgsConstructor;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.time.Duration;
import java.util.List;
import java.util.Set;

@Configuration
@EnableCaching
@RequiredArgsConstructor
public class CacheManagerConfig {
    private final AppConfig appConfig;

    @Bean
    @Primary
    public CacheManager cacheManager() {
        CaffeineCacheManager cacheManager = new CaffeineCacheManager();
        appConfig.getCache().timeouts()
                .forEach((key, duration) -> cacheManager.registerCustomCache(key,
                        Caffeine.newBuilder()
                                .initialCapacity(10)
                                .maximumSize(10000)
                                .expireAfterAccess(Duration.parse(duration))
                                .recordStats()
                                .build()));
        return cacheManager;
    }

    @Bean
    public CacheManager cacheManagerRedis() {
        CaffeineCacheManager cacheManager = new CaffeineCacheManager();
        appConfig.getCache().timeouts()
                .forEach((key, duration) -> cacheManager.registerCustomCache(key,
                        Caffeine.newBuilder()
                                .initialCapacity(10)
                                .maximumSize(10000)
                                .expireAfterAccess(Duration.parse(duration))
                                .recordStats()
                                .build()));
        return cacheManager;
    }

    public static class CacheOwnerKey {
        public static final String CONSUMER_GROUP_ALL_CACHE = "CONSUMER_GROUP_ALL_CACHE";
        public static final String CONSUMER_GROUP_BY_GROUP = "CONSUMER_GROUP_BY_GROUP";
        public static final String CONSUMER_GROUP_BY_TOPIC = "CONSUMER_GROUP_BY_TOPIC";
        public static final String CONSUMER_GROUPS_DESCRIBE = "CONSUMER_GROUPS_DESCRIBE";
        public static final String TOPIC_ALL = "TOPIC_ALL";
        public static final String TOPIC_INFO = "TOPIC_ONE";

    }

    @RestController
    @RequestMapping("admin/cache")
    @RequiredArgsConstructor
    public static class CacheController {
        private final CacheManager cacheManager;

        @GetMapping
        @Operation(hidden = true)
        public List<CacheInfo> getCacheInfo() {
            return cacheManager.getCacheNames()
                    .stream()
                    .map(this::getCacheInfo)
                    .toList();
        }

        @SuppressWarnings("unchecked")
        private CacheInfo getCacheInfo(String cacheName) {
            Cache<Object, Object> nativeCache = (Cache<Object, Object>) cacheManager.getCache(cacheName).getNativeCache();
            Set<Object> keys = nativeCache.asMap().keySet();
            CacheStats stats = nativeCache.stats();
            return new CacheInfo(cacheName, keys.size(), keys, stats.toString());
        }

        public record CacheInfo(
                String name, int size, Set<Object> keys, String stats) {
        }
    }
}
