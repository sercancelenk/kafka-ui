package com.trendyol.kafka.stream.api.service;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.trendyol.kafka.stream.api.controller.context.RequestContext;
import com.trendyol.kafka.stream.api.model.Models;
import com.trendyol.kafka.stream.api.service.manager.KafkaWrapper;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.acl.AclOperation;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
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
    private LoadingCache<String, List<Models.ConsumerGroupInfo>> consumerGroupCache;

    @PostConstruct
    public void init() {
        // Initialize Guava LoadingCache
        this.consumerGroupCache = CacheBuilder.newBuilder()
                .expireAfterWrite(20, TimeUnit.SECONDS)  // Cache entry expiration time
                .maximumSize(100)  // Maximum number of cached entries
                .build(new CacheLoader<>() {
                    @Override
                    public List<Models.ConsumerGroupInfo> load(String key) throws ExecutionException, InterruptedException {
                        //clusterid:cluster-1:topic:demo.topic.1
                        //clusterid:cluster-1:group:demo.topic.1
                        if (key.contains(":topic:")) {
                            String[] splittedKey = key.split(":");
                            String clusterId = splittedKey[1];
                            String topic = splittedKey[3];

                            return getConsumerGroupInfoByTopicUncached(topic);
                        } else {
                            String[] splittedKey = key.split(":");
                            String clusterId = splittedKey[1];
                            String group = splittedKey[3];
                            return getConsumerGroupInfoByGroupIdUncached(group);
                        }
                    }
                });
    }

    public List<Models.ConsumerGroupInfo> getConsumerGroupInfoByGroupId(String groupId) throws ExecutionException {
        return consumerGroupCache.get("clusterid:" + RequestContext.getClusterId() + ":group:" + groupId);  // Cache key is "clusterid:"+clusterId+":group:" + group
    }

    public List<Models.ConsumerGroupInfo> getConsumerGroupInfoByTopic(String topic) throws ExecutionException {
        return consumerGroupCache.get("clusterid:" + RequestContext.getClusterId() + ":topic:" + topic);
    }

    public List<Models.ConsumerGroupInfo> getConsumerGroupInfoByGroupIdUncached(String groupId) throws ExecutionException, InterruptedException {
        log.info("ConsumerGroupInfo getting from cluster {}", RequestContext.getClusterId());
        List<Models.ConsumerGroupInfo> result = new ArrayList<>();
        result.add(getConsumerGroupInfo(groupId, Optional.empty()));
        return result;
    }

    public List<Models.ConsumerGroupInfo> getConsumerGroupInfoByTopicUncached(String topic) {
        return getConsumerGroupsByTopic(topic)
                .stream()
                .map(groupId -> getConsumerGroupInfo(groupId, Optional.of(topic)))
                .toList();
    }

    public Set<String> getConsumerGroupsByTopic(String topic) {
        return kafkaWrapper.listConsumerGroupIds(RequestContext.getClusterId())
                .stream()
                .filter(groupId -> {
                    ConsumerGroupDescription groupDescription = kafkaWrapper.describeConsumerGroupsBy(RequestContext.getClusterId(), groupId);
                    return groupDescription.members().stream()
                            .anyMatch(member -> member.assignment().topicPartitions().stream()
                                    .anyMatch(tp -> tp.topic().equals(topic)));
                }).collect(Collectors.toSet());
    }

    public Models.ConsumerGroupInfo getConsumerGroupInfo(String groupId, Optional<String> topicOpt) {
        ConsumerGroupDescription groupDescription = kafkaWrapper.describeConsumerGroupsBy(RequestContext.getClusterId(), groupId);

        AtomicLong totalCommitted = new AtomicLong(0L);
        AtomicLong totalLatest = new AtomicLong(0L);
        AtomicLong totalLag = new AtomicLong(0L);
        AtomicInteger podCount = new AtomicInteger(0);



        List<Models.ConsumerGroupMember> members = groupDescription
                .members()
                .stream()
                .flatMap(member -> member.assignment()
                        .topicPartitions()
                        .stream()
                        .filter(f -> topicOpt.isEmpty() || f.topic().equals(topicOpt.get()))
                        .map(tp -> {
                            try {
                                OffsetAndMetadata committedOffset = kafkaWrapper.getCommittedOffset(RequestContext.getClusterId(), groupId, tp);
                                long latestOffset = kafkaWrapper.getLatestOffset(RequestContext.getClusterId(), tp);

                                long committed = committedOffset != null ? committedOffset.offset() : 0;
                                long lag = latestOffset - committed;

                                totalCommitted.addAndGet(committed);
                                totalLatest.addAndGet(latestOffset);
                                totalLag.addAndGet(lag);
                                podCount.addAndGet(1);

                                // Fill in the member's details
                                return Models.ConsumerGroupMember
                                        .builder()
                                        .lag(lag)
                                        .memberId(member.consumerId())
                                        .clientId(member.clientId())
                                        .host(member.host())
                                        .topic(tp.topic())
                                        .partition(tp.partition())
                                        .committedOffset(committed)
                                        .latestOffset(latestOffset)
                                        .build();
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        })
                        .toList().stream()
                ).toList();

        return Models.ConsumerGroupInfo
                .builder()
                .groupId(groupId)
                .state(groupDescription.state().toString())
                .partitionAssignor(StringUtils.isEmpty(groupDescription.partitionAssignor()) ? "NONE" : groupDescription.partitionAssignor())
                .members(members)
                .totalCommitted(totalCommitted.get())
                .totalLatest(totalLatest.get())
                .totalLag(totalLag.get())
                .memberCount(podCount.get())
                .podCount(podCount.get())
                .build();

    }

    public Models.PaginatedResponse<String> getConsumerGroupIdsPaginated(int page, int size) {
        List<String> consumerGroups = kafkaWrapper.listConsumerGroupIds(RequestContext.getClusterId());

        int totalItems = consumerGroups.size();
        int totalPages = (int) Math.ceil((double) totalItems / size);

        int start = Math.min(page * size, totalItems);
        int end = Math.min((page * size) + size, totalItems);

        List<String> paginatedConsumerGroupIds = consumerGroups.subList(start, end)
                .stream()
                .toList();

        return new Models.PaginatedResponse<>(paginatedConsumerGroupIds, page, totalItems, totalPages);
    }
}