package com.trendyol.kafka.stream.api.adapters.cache.guava;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.trendyol.kafka.stream.api.domain.Exceptions;
import lombok.Getter;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Service
public class CacheService {
    Map<CacheOwnerKey, LoadingCache<String, ?>> cachesMap = new ConcurrentHashMap<>();

    public <V> V getFromCache(CacheOwnerKey ownerKey, Object ... params) {
        String key = String.format(ownerKey.keyFormat, params);
        LoadingCache<String, V> loadingCache = (LoadingCache<String, V>) cachesMap.get(ownerKey);
        try {
            return loadingCache.get(key);
        } catch (ExecutionException e) {
            throw new Exceptions.ProcessExecutionException(e);
        }
    }

    public <V> void createCache(CacheOwnerKey ownerKey, long ttl, TimeUnit duration, CacheLoader<String, V> cacheLoader) {
        LoadingCache<String, V> c = CacheBuilder.newBuilder()
                .expireAfterWrite(ttl, duration)  // Cache entry expiration time
                .build(cacheLoader);

        cachesMap.putIfAbsent(ownerKey, c);
    }

    public <K, V> LoadingCache<K, V> createCache(CacheLoader<K, V> cacheLoader, long ttl, long maximumSize) {
        return CacheBuilder.newBuilder()
                .expireAfterWrite(ttl, TimeUnit.SECONDS)  // Use dynamic TTL
                .maximumSize(maximumSize)  // Use dynamic cache size
                .build(cacheLoader);  // Use the provided CacheLoader
    }

    public enum CacheOwnerKey {
        CONSUMER_GROUP_ALL_CACHE("allgroups:clusterid:%s"),
        CONSUMER_GROUP_BY_GROUP("consumergroup:clusterid:%s:group:%s"),
        CONSUMER_GROUP_BY_TOPIC("consumergroups:clusterid:%s:topic:%s"),
        TOPIC_INFO("topicinfo:clusterid:%s:topic:%s"),
        TOPICS_ALL("topics:clusterid:%s");

        public final String keyFormat;

        CacheOwnerKey(String keyformat) {
            this.keyFormat = keyformat;
        }
    }
}

