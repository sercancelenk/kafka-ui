package com.trendyol.kafka.stream.api.service;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.trendyol.kafka.stream.api.controller.context.RequestContext;
import com.trendyol.kafka.stream.api.model.Models;
import com.trendyol.kafka.stream.api.service.manager.KafkaWrapper;
import com.trendyol.kafka.stream.api.util.ThreadLocalPriorityQueue;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;

import javax.annotation.Nonnull;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class TopicService {
    private final Map<String, Models.ClusterAdminInfo> clusterAdminMap;
    private final KafkaWrapper kafkaWrapper;
    private final Integer cacheTimeoutSeconds = 5;
    private LoadingCache<String, Models.TopicInfo> topicInfoCache;
    private LoadingCache<String, List<String>> allTopicNameListCache;

    @PostConstruct
    public void init() {
        topicInfoCache = CacheBuilder.newBuilder()
                .expireAfterWrite(cacheTimeoutSeconds, TimeUnit.SECONDS)  // Cache entry expiration time
                .maximumSize(100)  // Maximum number of cache entries
                .build(new CacheLoader<>() {
                    @Override
                    @Nonnull
                    public Models.TopicInfo load(@NonNull String compositeKey) throws ExecutionException, InterruptedException {
                        String[] keyParts = compositeKey.split(":", 2);
                        String topicName = keyParts[1];
                        return getTopicInfoUnCached(topicName);
                    }
                });

        allTopicNameListCache = CacheBuilder.newBuilder()
                .expireAfterWrite(cacheTimeoutSeconds, TimeUnit.SECONDS)  // Cache entry expiration time
                .maximumSize(100)  // Maximum number of cache entries
                .build(new CacheLoader<>() {
                    @Override
                    @Nonnull
                    public List<String> load(@Nonnull String allTopicsByClusterIdKey) {
                        String[] keyParts = allTopicsByClusterIdKey.split(":", 2);
                        String clusterId = keyParts[1];
                        return kafkaWrapper.getAllTopics(clusterId, true);
                    }
                });
    }

    public List<Models.MessageInfo> getTopMessagesFromTopic(String topic, int maxMessagesToFetch) throws InterruptedException {
        KafkaConsumer<String, String> consumer = null;
        try {
            PriorityQueue<Models.MessageInfo> topMessagesQueue = ThreadLocalPriorityQueue.getPriorityQueue();
            consumer = clusterAdminMap.get(RequestContext.getClusterId()).consumerPool().borrowConsumer();

            List<TopicPartition> partitions = kafkaWrapper.getTopicPartitions(topic, consumer);

            kafkaWrapper.assignPartitionsToConsumer(consumer, partitions);

            kafkaWrapper.seekConsumerTo(maxMessagesToFetch, consumer, partitions);

            topMessagesQueue.clear();

            // Poll messages from Kafka, limit the number of messages to the requested amount
            while (topMessagesQueue.size() < maxMessagesToFetch) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));  // Poll every 1 second

                for (ConsumerRecord<String, String> record : records) {
                    Models.MessageInfo messageInfo = kafkaWrapper.extractMessage(record);

                    // Add the message to the priority queue
                    topMessagesQueue.offer(messageInfo);
                    if (topMessagesQueue.size() > maxMessagesToFetch) {
                        topMessagesQueue.poll();
                    }
                }

                // Break if no more messages are available
                if (records.isEmpty()) {
                    break;
                }
            }

            // Sort messages by size and return the top N
            List<Models.MessageInfo> sortedMessages = new ArrayList<>(topMessagesQueue);
            sortedMessages.sort(Comparator.comparingLong(Models.MessageInfo::offset).reversed());

            return sortedMessages;
        } finally {
            // Return the consumer to the pool after processing
            if (consumer != null) {
                clusterAdminMap.get(RequestContext.getClusterId()).consumerPool().returnConsumer(consumer);
            }
        }
    }

    public Models.MessageInfo getOneMessageFromTopic(String topic, Optional<Integer> partitionOpt, Optional<Long> offsetOpt) throws InterruptedException {
        KafkaConsumer<String, String> consumer = null;
        try {
            consumer = clusterAdminMap.get(RequestContext.getClusterId()).consumerPool().borrowConsumer();

            // If partition is provided
            if (partitionOpt.isPresent()) {
                int partition = partitionOpt.get();
                TopicPartition topicPartition = new TopicPartition(topic, partition);

                kafkaWrapper.assignPartitionsToConsumer(consumer, Collections.singletonList(topicPartition));

                // If offset is provided, seek to that offset, otherwise seek to the end (last message)
                if (offsetOpt.isPresent()) {
                    kafkaWrapper.seekConsumerSpecificPartitionAndOffset(offsetOpt, consumer, topicPartition);
                } else kafkaWrapper.seekConsumerSpecificPartitionToEnd(consumer, topicPartition);

            } else kafkaWrapper.seekConsumerAllPartitionsToEnd(topic, consumer);

            // Poll for messages
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000)); // Poll for 1 second
            if (!records.isEmpty()) {
                // Return the first message in the records
                ConsumerRecord<String, String> record = records.iterator().next();
                return kafkaWrapper.extractMessage(record);
            }
            return Models.MessageInfo.builder().build();

        } finally {
            // Return the consumer to the pool after processing
            if (consumer != null) {
                clusterAdminMap.get(RequestContext.getClusterId()).consumerPool().returnConsumer(consumer);
            }
        }
    }

    public Models.TopicInfo getTopicInfo(String topicName) throws ExecutionException {
        String compositeKey = RequestContext.getClusterId() + ":" + topicName;
        return topicInfoCache.get(compositeKey);
    }

    public Map<String, Long> getLagCount(String topicName, String groupId) throws ExecutionException, InterruptedException {
        try (AdminClient adminClient = clusterAdminMap.get(RequestContext.getClusterId()).adminClient()) {
            Map<String, Long> lagCount = new HashMap<>();

            TopicDescription topicDescription = adminClient.describeTopics(Collections.singletonList(topicName))
                    .topicNameValues()
                    .get(topicName)
                    .get();

            for (TopicPartition partition : topicDescription.partitions().stream()
                    .map(p -> new TopicPartition(topicName, p.partition()))
                    .toList()) {

                // Get the committed offset for the group
                OffsetAndMetadata committedOffset = adminClient.listConsumerGroupOffsets(groupId)
                        .partitionsToOffsetAndMetadata()
                        .get()
                        .get(partition);

                if (committedOffset != null) {
                    ListOffsetsResult latestOffsetResult = adminClient.listOffsets(Collections.singletonMap(partition, OffsetSpec.latest()));
                    long latestOffset = latestOffsetResult.partitionResult(partition).get().offset();

                    long lag = latestOffset - committedOffset.offset();

                    lagCount.put("Partition " + partition.partition(), lag);
                }
            }
            return lagCount;
        }
    }

    private Models.TopicInfo getTopicInfoUnCached(String topicName) throws ExecutionException, InterruptedException {
        Models.TopicInfo.TopicInfoBuilder builder = Models.TopicInfo.builder();
        Map<String, Object> topicConfig = new HashMap<>();

        String clusterId = RequestContext.getClusterId();

        builder.clusterId(clusterId);
        builder.name(topicName);

        // Get the topic description to fetch partition information
        TopicDescription topicDescription = kafkaWrapper.describeTopic(clusterId, topicName);
        int partitionCount = topicDescription.partitions().size();
        builder.partitionCount(partitionCount);

        // Fetch the retention period
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        Config config = clusterAdminMap.get(RequestContext.getClusterId()).adminClient()
                .describeConfigs(Collections.singletonList(configResource))
                .values()
                .get(configResource)
                .get();

        String retentionMs = config.get(TopicConfig.RETENTION_MS_CONFIG).value();
        long retentionDay = Long.parseLong(retentionMs) / (1000 * 60 * 60 * 24);  // Convert retention in ms to days
        builder.retentionDay(retentionDay);

        // Calculate total message count
        long totalMessageCount = 0;
        Map<String, Integer> replicas = new ConcurrentHashMap<>();
        for (TopicPartitionInfo tpi : topicDescription.partitions()) {
            TopicPartition partition = new TopicPartition(topicName, tpi.partition());
            ListOffsetsResult.ListOffsetsResultInfo earliest = clusterAdminMap.get(RequestContext.getClusterId()).adminClient().listOffsets(
                    Collections.singletonMap(partition, OffsetSpec.earliest())).partitionResult(partition).get();
            ListOffsetsResult.ListOffsetsResultInfo latest = clusterAdminMap.get(RequestContext.getClusterId()).adminClient().listOffsets(
                    Collections.singletonMap(partition, OffsetSpec.latest())).partitionResult(partition).get();

            long partitionMessageCount = latest.offset() - earliest.offset();
            totalMessageCount += partitionMessageCount;
            tpi.replicas().forEach(node -> replicas.putIfAbsent(node.host() + ":" +node.port(), 1));
        }
        builder.messageCount(totalMessageCount);
        builder.replicas(replicas.size());

        config.entries().forEach(entry -> topicConfig.putIfAbsent(entry.name(), entry.value()));
        builder.config(topicConfig);

        return builder.build();
    }

    public Models.PaginatedResponse<String> getTopicListUnCached(int page, int size) throws ExecutionException, InterruptedException {
        List<String> allTopics = allTopicNameListCache.get("allTopicNames:" + RequestContext.getClusterId());

        int totalItems = allTopics.size();
        int totalPages = (int) Math.ceil((double) totalItems / size);

        int start = Math.min(page * size, totalItems);
        int end = Math.min((page * size) + size, totalItems);

        if (start > allTopics.size()) {
            return new Models.PaginatedResponse<>(List.of(), 0, 0, 0);
        }

        return new Models.PaginatedResponse<>(allTopics.subList(start, end), page, totalItems, totalPages);
    }

}
