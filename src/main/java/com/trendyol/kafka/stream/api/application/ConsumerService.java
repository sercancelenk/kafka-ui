package com.trendyol.kafka.stream.api.application;

import com.google.common.cache.CacheLoader;
import com.trendyol.kafka.stream.api.adapters.cache.guava.CacheService;
import com.trendyol.kafka.stream.api.adapters.kafka.manager.KafkaWrapper;
import com.trendyol.kafka.stream.api.domain.Models;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class ConsumerService {
    private final KafkaWrapper kafkaWrapper;
    private final Integer cacheTimeoutSeconds = 5;
    private final CacheService cacheService;

    @PostConstruct
    public void init() {
        cacheService.<List<String>>createCache(CacheService.CacheOwnerKey.CONSUMER_GROUP_ALL_CACHE,
                1, TimeUnit.MINUTES,
                new CacheLoader<>() {
                    @Override
                    public List<String> load(String key) throws ExecutionException, InterruptedException {
                        //allgroups:clusterid:cluster-1
                        String[] splittedKey = key.split(":");
                        String clusterId = splittedKey[2];
                        return kafkaWrapper.listConsumerGroupIds(clusterId);
                    }
                });

        cacheService.<List<Models.ConsumerGroupInfo>>createCache(CacheService.CacheOwnerKey.CONSUMER_GROUP_BY_TOPIC,
                1, TimeUnit.MINUTES,
                new CacheLoader<>() {
                    @Override
                    public List<Models.ConsumerGroupInfo> load(String key) throws ExecutionException, InterruptedException {
                        //consumergroups:clusterid:%s:topic:%s
                        String[] splittedKey = key.split(":");
                        String clusterId = splittedKey[2];
                        String topic = splittedKey[4];
                        return getConsumerGroupInfoByTopicUncached(clusterId, topic);
                    }
                });

        cacheService.<List<Models.ConsumerGroupInfo>>createCache(CacheService.CacheOwnerKey.CONSUMER_GROUP_BY_GROUP,
                1, TimeUnit.MINUTES,
                new CacheLoader<>() {
                    @Override
                    public List<Models.ConsumerGroupInfo> load(String key) throws ExecutionException, InterruptedException {
                        //consumergroups:clusterid:%s:topic:%s
                        String[] splittedKey = key.split(":");
                        String clusterId = splittedKey[2];
                        String topic = splittedKey[4];
                        return getConsumerGroupInfoByGroupIdUncached(clusterId, topic);
                    }
                });
    }

    public List<Models.ConsumerGroupInfo> getConsumerGroupInfoByGroupId(String clusterId, String groupId) {
        return cacheService.getFromCache(CacheService.CacheOwnerKey.CONSUMER_GROUP_BY_GROUP, clusterId, groupId);
    }

    public List<Models.ConsumerGroupInfo> getConsumerGroupInfoByTopic(String clusterId, String topic) {
        return cacheService.getFromCache(CacheService.CacheOwnerKey.CONSUMER_GROUP_BY_GROUP,clusterId, topic);
    }

    public List<Models.ConsumerGroupInfo> getConsumerGroupInfoByGroupIdUncached(String clusterId, String groupId) {
        log.info("ConsumerGroupInfo getting from cluster {}", clusterId);
        return List.of(extractConsumerGroupInfo(clusterId, kafkaWrapper.getSingleConsumerGroupDescription(clusterId, groupId)));
    }

    public List<Models.ConsumerGroupInfo> getConsumerGroupInfoByTopicUncached(String clusterId, String topic) {
        return kafkaWrapper.listConsumerGroupIds(clusterId)
                .stream()
                .filter(groupId -> kafkaWrapper.isOffsetBelongsToTopicInGroup(clusterId, groupId, topic))
                .map(groupId -> extractConsumerGroupInfo(clusterId, kafkaWrapper.getSingleConsumerGroupDescription(clusterId, groupId)))
                .toList();
    }

    public Models.ConsumerGroupInfo extractConsumerGroupInfo(String clusterId, ConsumerGroupDescription groupDescription) {
        String groupId = groupDescription.groupId();
        AtomicInteger totalMemberCount = new AtomicInteger(0);
        AtomicLong totalLag = new AtomicLong(0L);
        Map<String, Integer> podMap = new ConcurrentHashMap<>();
        AtomicInteger totalAssignedPartitions = new AtomicInteger(0);

        Models.ConsumerGroupCoordinator coordinator = Models.ConsumerGroupCoordinator
                .builder()
                .id(groupDescription.coordinator().id())
                .idString(groupDescription.coordinator().idString())
                .host(groupDescription.coordinator().host())
                .port(groupDescription.coordinator().port())
                .rack(groupDescription.coordinator().rack())
                .build();

        Map<String, List<Models.ConsumerGroupMember>> membersByTopic = groupDescription
                .members()
                .stream()
                .flatMap(member -> member.assignment()
                        .topicPartitions()
                        .stream()
                        .map(tp -> {
                            OffsetAndMetadata committedOffset = kafkaWrapper.getCommittedOffset(clusterId, groupId, tp);
                            long latestOffset = kafkaWrapper.getLatestOffset(clusterId, tp);

                            long committed = committedOffset != null ? committedOffset.offset() : 0;
                            long lag = latestOffset - committed;

                            totalLag.addAndGet(lag);

                            totalAssignedPartitions.incrementAndGet();
                            podMap.putIfAbsent(member.host(), 1);

                            return Models.ConsumerGroupMember
                                    .builder()
                                    .lag(lag)
                                    .consumerId(member.consumerId())
                                    .memberId(member.consumerId())
                                    .clientId(member.clientId())
                                    .host(member.host())
                                    .topic(tp.topic())
                                    .partition(tp.partition())
                                    .committedOffset(committed)
                                    .latestOffset(latestOffset)
                                    .build();

                        }).toList().stream()
                ).collect(Collectors.groupingBy(Models.ConsumerGroupMember::topic));

        return Models.ConsumerGroupInfo
                .builder()
                .groupId(groupId)
                .coordinator(coordinator)
                .state(groupDescription.state().toString())
                .partitionAssignor(StringUtils.isEmpty(groupDescription.partitionAssignor()) ? "NONE" : groupDescription.partitionAssignor())
                .membersByTopic(membersByTopic)
                .memberCount(podMap.size())
                .podCount(podMap.size())
                .assignedTopicCount(membersByTopic.keySet().size())
                .assignedPartitionsCount(totalAssignedPartitions.get())
                .totalLag(totalLag.get())
                .build();

    }

    //    @CircuitBreaker(name = "getConsumerGroups", fallbackMethod = "getConsumerGroupsFallback")
    public Models.PaginatedResponse<String> getConsumerGroupIdsPaginated(String clusterId, int page, int size) {
        List<String> consumerGroups = cacheService.getFromCache(CacheService.CacheOwnerKey.CONSUMER_GROUP_ALL_CACHE,
                clusterId);

        int totalItems = consumerGroups.size();
        int totalPages = (int) Math.ceil((double) totalItems / size);

        int start = Math.min(page * size, totalItems);
        int end = Math.min((page * size) + size, totalItems);

        List<String> paginatedConsumerGroupIds = consumerGroups.subList(start, end)
                .stream()
                .toList();

        return new Models.PaginatedResponse<>(paginatedConsumerGroupIds, page, totalItems, totalPages);
    }

    public Models.PaginatedResponse<String> getConsumerGroupsFallback(int page, int size, Throwable throwable) {
        System.out.println("Circuit breaker triggered for getConsumerGroups");
        return new Models.PaginatedResponse<String>(Collections.emptyList(), page, size, 0);
    }
}