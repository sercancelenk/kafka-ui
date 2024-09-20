package com.trendyol.kafka.stream.api.application;

import com.trendyol.kafka.stream.api.adapters.kafka.manager.KafkaWrapper;
import com.trendyol.kafka.stream.api.domain.Models;
import com.trendyol.kafka.stream.api.infra.CacheManagerConfig;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class ConsumerService {
    private final KafkaWrapper kafkaWrapper;

    @Cacheable(cacheNames = CacheManagerConfig.CacheOwnerKey.CONSUMER_GROUP_BY_GROUP, key = "{#root.methodName,#clusterId,#groupId}", unless = "#result == null")
    public List<Models.ConsumerGroupInfo> getConsumerGroupInfoByGroupId(String clusterId, String groupId) {
        log.info("ConsumerGroupInfo getting from cluster {}", clusterId);
        return List.of(extractConsumerGroupInfo(clusterId, kafkaWrapper.getSingleConsumerGroupDescription(clusterId, groupId)));
    }

    @Cacheable(cacheNames = CacheManagerConfig.CacheOwnerKey.CONSUMER_GROUP_BY_TOPIC, key = "{#root.methodName,#clusterId,#topic}", unless = "#result == null")
    public List<Models.ConsumerGroupInfo> getConsumerGroupInfoByTopic(String clusterId, String topic) {
        Map<String, ConsumerGroupDescription> groupDescribes = kafkaWrapper.describeConsumerGroups(clusterId);
        return groupDescribes
                .values()
                .stream()
                .filter(descr ->
                        descr.members().stream().
                                map(s -> s.assignment().topicPartitions()).
                                flatMap(Collection::stream).
                                anyMatch(s -> s.topic().equals(topic))
                )
                .map(descr -> extractConsumerGroupInfo(clusterId, descr))
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

    @CircuitBreaker(name = "getConsumerGroups", fallbackMethod = "getConsumerGroupsFallback")
    @Cacheable(cacheNames = CacheManagerConfig.CacheOwnerKey.CONSUMER_GROUP_ALL_CACHE, key = "{#root.methodName,#clusterId,#page,#size}", unless = "#result == null")
    public Models.PaginatedResponse<String> getConsumerGroupIdsPaginated(String clusterId, int page, int size) {
        List<String> consumerGroups = kafkaWrapper.listConsumerGroupIds(clusterId);

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