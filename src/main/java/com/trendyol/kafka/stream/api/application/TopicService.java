package com.trendyol.kafka.stream.api.application;

import com.google.common.cache.CacheLoader;
import com.trendyol.kafka.stream.api.adapters.cache.guava.CacheService;
import com.trendyol.kafka.stream.api.adapters.kafka.manager.KafkaWrapper;
import com.trendyol.kafka.stream.api.domain.Models;
import com.trendyol.kafka.stream.api.infra.utils.ForkJoinSupport;
import com.trendyol.kafka.stream.api.infra.utils.ThreadLocalPriorityQueue;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
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
import java.util.concurrent.atomic.AtomicLong;

@Service
@RequiredArgsConstructor
@Slf4j
public class TopicService {
    private final Map<String, Models.ClusterAdminInfo> clusterAdminMap;
    private final KafkaWrapper kafkaWrapper;
    private final Integer cacheTimeoutSeconds = 5;
    private final CacheService cacheService;

    @PostConstruct
    public void init() {
        cacheService.createCache(CacheService.CacheOwnerKey.TOPIC_INFO,
                cacheTimeoutSeconds, TimeUnit.SECONDS,
                new CacheLoader<>() {
                    @Override
                    @Nonnull
                    public Models.TopicInfo load(@NonNull String compositeKey) throws ExecutionException, InterruptedException {
                        String[] keyParts = compositeKey.split(":", 5);
                        String clusterId = keyParts[2];
                        String topicName = keyParts[4];
                        return getTopicInfoUnCached(clusterId, topicName);
                    }
                });
        cacheService.createCache(CacheService.CacheOwnerKey.TOPICS_ALL,
                cacheTimeoutSeconds, TimeUnit.SECONDS,
                new CacheLoader<>() {
                    @Override
                    @Nonnull
                    public List<String> load(@Nonnull String allTopicsByClusterIdKey) {
                        String[] keyParts = allTopicsByClusterIdKey.split(":", 3);
                        String clusterId = keyParts[2];
                        return kafkaWrapper.getAllTopics(clusterId, true);
                    }
                });

    }

    public List<Models.MessageInfo> getTopMessagesFromTopic(String clusterId, String topic, int maxMessagesToFetch) throws InterruptedException {
        KafkaConsumer<String, String> consumer = null;
        try {
            PriorityQueue<Models.MessageInfo> topMessagesQueue = ThreadLocalPriorityQueue.getPriorityQueue();
            consumer = clusterAdminMap.get(clusterId).consumerPool().borrowConsumer();

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
                clusterAdminMap.get(clusterId).consumerPool().returnConsumer(consumer);
            }
        }
    }

    public Models.MessageInfo getOneMessageFromTopic(String clusterId, String topic, Optional<Integer> partitionOpt, Optional<Long> offsetOpt) throws InterruptedException {
        KafkaConsumer<String, String> consumer = null;
        try {
            consumer = clusterAdminMap.get(clusterId).consumerPool().borrowConsumer();

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
                clusterAdminMap.get(clusterId).consumerPool().returnConsumer(consumer);
            }
        }
    }

    public Models.TopicInfo getTopicInfo(String clusterId, String topicName) throws ExecutionException {
        return cacheService.getFromCache(CacheService.CacheOwnerKey.TOPIC_INFO, clusterId, topicName);
    }

    public Map<String, Long> getLagCount(String clusterId, String topicName, String groupId) throws ExecutionException, InterruptedException {
        try (AdminClient adminClient = clusterAdminMap.get(clusterId).adminClient()) {
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

    private Models.TopicInfo getTopicInfoUnCached(String clusterId, String topicName) throws ExecutionException, InterruptedException {
        Models.TopicInfo.TopicInfoBuilder builder = Models.TopicInfo.builder();
        Map<String, Object> topicConfig = new HashMap<>();

        builder.clusterId(clusterId);
        builder.name(topicName);

        // Get the topic description to fetch partition information
        TopicDescription topicDescription = kafkaWrapper.describeTopic(clusterId, topicName);
        int partitionCount = topicDescription.partitions().size();
        builder.partitionCount(partitionCount);

        // Fetch the retention period
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);
        Config config = clusterAdminMap.get(clusterId).adminClient()
                .describeConfigs(Collections.singletonList(configResource))
                .values()
                .get(configResource)
                .get();

        String retentionMs = config.get(TopicConfig.RETENTION_MS_CONFIG).value();
        long retentionDay = Long.parseLong(retentionMs) / (1000 * 60 * 60 * 24);  // Convert retention in ms to days
        builder.retentionDay(retentionDay);

        // Calculate total message count
        AtomicLong totalMessageCount = new AtomicLong(0L);
        Map<String, Integer> replicas = new ConcurrentHashMap<>();

        ForkJoinSupport.execute((tpi) -> {
            try {
                TopicPartition partition = new TopicPartition(topicName, tpi.partition());
                ListOffsetsResult.ListOffsetsResultInfo earliest = clusterAdminMap.get(clusterId).adminClient().listOffsets(
                        Collections.singletonMap(partition, OffsetSpec.earliest())).partitionResult(partition).get();
                ListOffsetsResult.ListOffsetsResultInfo latest = clusterAdminMap.get(clusterId).adminClient().listOffsets(
                        Collections.singletonMap(partition, OffsetSpec.latest())).partitionResult(partition).get();

                long partitionMessageCount = latest.offset() - earliest.offset();
                totalMessageCount.addAndGet(partitionMessageCount);
                tpi.replicas().forEach(node -> replicas.putIfAbsent(node.host() + ":" + node.port(), 1));
                log.info("Execute func is finished for partition {}", tpi.partition());
            } catch (Exception ex) {
                log.info("Execute func has an error while processing the partition {}", tpi.partition(), ex);
            }
        }, topicDescription.partitions());

        builder.messageCount(totalMessageCount.get());
        builder.replicas(replicas.size());

        config.entries().forEach(entry -> topicConfig.putIfAbsent(entry.name(), entry.value()));
        builder.config(topicConfig);

        return builder.build();
    }

    public Models.PaginatedResponse<String> getTopicListUnCached(String clusterId, int page, int size) throws ExecutionException {
        List<String> allTopics = cacheService.getFromCache(CacheService.CacheOwnerKey.TOPICS_ALL, clusterId);

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
