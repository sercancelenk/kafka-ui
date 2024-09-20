package com.trendyol.kafka.stream.api.application;

import com.trendyol.kafka.stream.api.adapters.kafka.manager.KafkaWrapper;
import com.trendyol.kafka.stream.api.domain.Models;
import com.trendyol.kafka.stream.api.infra.CacheManagerConfig;
import com.trendyol.kafka.stream.api.infra.utils.ThreadLocalPriorityQueue;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.TopicConfig;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

@Service
@RequiredArgsConstructor
@Slf4j
public class TopicService {
    private final KafkaWrapper kafkaWrapper;
    private final Integer cacheTimeoutSeconds = 1;

    public List<Models.MessageInfo> getTopMessagesFromTopic(String clusterId, String topic, int maxMessagesToFetch) throws InterruptedException {
        KafkaConsumer<String, String> consumer = null;
        try {
            PriorityQueue<Models.MessageInfo> topMessagesQueue = ThreadLocalPriorityQueue.getPriorityQueue();
            consumer = kafkaWrapper.getConsumerPool(clusterId).borrowConsumer();

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
                kafkaWrapper.getConsumerPool(clusterId).returnConsumer(consumer);
            }
        }
    }

    public Models.MessageInfo getOneMessageFromTopic(String clusterId, String topic, Optional<Integer> partitionOpt, Optional<Long> offsetOpt) throws InterruptedException {
        KafkaConsumer<String, String> consumer = null;
        try {
            consumer = kafkaWrapper.getConsumerPool(clusterId).borrowConsumer();

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
                kafkaWrapper.getConsumerPool(clusterId).returnConsumer(consumer);
            }
        }
    }

    public Map<String, Long> getLagCount(String clusterId, String topicName, String groupId) throws ExecutionException, InterruptedException {
        try {
            AdminClient adminClient = kafkaWrapper.getAdminClient(clusterId);
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
        } catch (Exception ex) {
            log.error("Exception occurred while getting lag. {}", ex.getMessage(), ex);
            return Map.of();
        }
    }


    @Cacheable(cacheNames = CacheManagerConfig.CacheOwnerKey.TOPIC_INFO, key = "{#root.methodName,#clusterId,#topic}", unless = "#result == null")
    public Models.TopicInfo getTopicInfo(String clusterId, String topic) {
        Models.TopicInfo.TopicInfoBuilder builder = Models.TopicInfo.builder();
        Map<String, Object> topicConfig = new HashMap<>();

        builder.clusterId(clusterId);
        builder.name(topic);

        // Get the topic description to fetch partition information
        TopicDescription topicDescription = kafkaWrapper.describeTopic(clusterId, topic);
        int partitionCount = topicDescription.partitions().size();
        builder.partitionCount(partitionCount);


        // Fetch the retention period
        Config config = kafkaWrapper.describeTopicConfig(clusterId, topic);

        String retentionMs = config.get(TopicConfig.RETENTION_MS_CONFIG).value();
        long retentionDay = Long.parseLong(retentionMs) / (1000 * 60 * 60 * 24);  // Convert retention in ms to days
        builder.retentionDay(retentionDay);

        // Calculate total message count
        AtomicLong totalMessageCount = new AtomicLong(0L);
        Map<String, Integer> replicas = new ConcurrentHashMap<>();

        for (TopicPartitionInfo tpi : topicDescription.partitions()) {
            TopicPartition partition = new TopicPartition(topic, tpi.partition());
            ListOffsetsResult.ListOffsetsResultInfo earliest = kafkaWrapper.listOffsets(clusterId, partition, OffsetSpec.earliest());
            ListOffsetsResult.ListOffsetsResultInfo latest = kafkaWrapper.listOffsets(clusterId, partition, OffsetSpec.latest());

            long partitionMessageCount = latest.offset() - earliest.offset();
            totalMessageCount.addAndGet(partitionMessageCount);
            tpi.replicas().forEach(node -> replicas.putIfAbsent(node.host() + ":" + node.port(), 1));
        }

        builder.messageCount(totalMessageCount.get());
        builder.replicas(replicas.size());

        config.entries().forEach(entry -> topicConfig.putIfAbsent(entry.name(), entry.value()));
        builder.config(topicConfig);

        return builder.build();
    }


    public Models.PaginatedResponse<String> getTopicList(String clusterId, int page, int size) throws ExecutionException {
        List<String> allTopics = kafkaWrapper.getAllTopics(clusterId, true);

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
