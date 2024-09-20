package com.trendyol.kafka.stream.api.adapters.kafka.manager;

import com.trendyol.kafka.stream.api.domain.Exceptions;
import com.trendyol.kafka.stream.api.domain.Models;
import com.trendyol.kafka.stream.api.infra.CacheManagerConfig;
import com.trendyol.kafka.stream.api.infra.utils.Future;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

@Service
@RequiredArgsConstructor
public class KafkaWrapper {
    private final Map<String, Models.ClusterAdminInfo> clusterAdminManager;
    private final ClusterInfoConfig clusterInfoConfig;
    private final AdminClientService adminClientService;
    private final Long timeoutSeconds = 20L;

    public Models.ClusterAdminInfo getClusterAdmin(String clusterId) {
        if (clusterAdminManager.containsKey(clusterId)) {
            return clusterAdminManager.get(clusterId);
        }
        throw Exceptions.ClusterNotSupportedException.builder().params(new Object[]{clusterId}).build();
    }

    public AdminClient getAdminClient(String clusterId) {
        return clusterAdminManager.get(clusterId).adminClient();
    }

    public ConsumerPool getConsumerPool(String clusterId) {
        return clusterAdminManager.get(clusterId).consumerPool();
    }

    public Config describeTopicConfig(String clusterId, String topic){
        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
        return Future.call(getAdminClient(clusterId)
                .describeConfigs(Collections.singletonList(configResource))
                .values()
                .get(configResource));
    }

    public ListOffsetsResult.ListOffsetsResultInfo listOffsets(String clusterId, TopicPartition partition, OffsetSpec offsetSpec) {
        return Future.call(
                getAdminClient(clusterId).listOffsets(
                        Collections.singletonMap(partition, offsetSpec)).partitionResult(partition)
        );
    }

    public ConsumerGroupDescription getSingleConsumerGroupDescription(String clusterId, String groupId) {
        try {
            return getAdminClient(clusterId)
                    .describeConsumerGroups(Collections.singletonList(groupId))
                    .describedGroups()
                    .get(groupId)
                    .get();
        } catch (InterruptedException | ExecutionException e) {
            throw new Exceptions.ProcessExecutionException(e);
        }
    }


    public Models.MessageInfo extractMessage(ConsumerRecord<String, String> record) {
        String messageValue = record.value();
        int sizeInBytes = messageValue.getBytes().length;
        double sizeInKB = (double) sizeInBytes / 1024;
        double sizeInMb = (double) sizeInBytes / (1024 * 1024);

        Map<String, String> headers = getHeadersAsMap(record);

        return Models.MessageInfo
                .builder()
                .sizeInBytes(sizeInBytes)
                .sizeInKB(sizeInKB)
                .sizeInMb(sizeInMb)
                .key(record.key())
                .value(record.value())
                .headers(headers)
                .topic(record.topic())
                .partition(record.partition())
                .offset(record.offset())
                .timestamp(record.timestamp())
                .timestampType(record.timestampType().name)
                .build();
    }

    public Map<String, String> getHeadersAsMap(ConsumerRecord<String, String> record) {
        Headers headers = record.headers();
        Map<String, String> headersMap = new HashMap<>();
        for (Header header : headers) {
            headersMap.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
        }
        return headersMap;
    }

    public List<String> listConsumerGroupIds(String clusterId) {
        try {
            return getAdminClient(clusterId)
                    .listConsumerGroups()
                    .all()
                    .get()
                    .stream().map(ConsumerGroupListing::groupId)
                    .toList();
        } catch (InterruptedException | ExecutionException e) {
            throw new Exceptions.ProcessExecutionException(e);
        }
    }

    public boolean isOffsetBelongsToTopicInGroup(String clusterId, String groupId, String topic) {
        return Future.call(getAdminClient(clusterId).listConsumerGroupOffsets(groupId)
                        .partitionsToOffsetAndMetadata())
                .entrySet().stream()
                .anyMatch(entry -> entry.getKey().topic().equals(topic));

    }

    public OffsetAndMetadata getCommittedOffset(String clusterId, String groupId, TopicPartition tp) {
        return Future.call(
                        getAdminClient(clusterId)
                                .listConsumerGroupOffsets(groupId)
                                .partitionsToOffsetAndMetadata())
                .get(tp);
    }

    // Method to get message counts per partition
    public long getMessageCount(String clusterId, String topic) throws InterruptedException, ExecutionException {
        AtomicLong totalMessageCount = new AtomicLong(0);
        try (KafkaConsumer<String, String> consumer = getConsumerPool(clusterId).borrowConsumer()) {
            // Get partition information for the topic
            List<PartitionInfo> partitionInfos = consumer.partitionsFor(topic);

            // Create TopicPartition objects for each partition
            List<TopicPartition> topicPartitions = partitionInfos.stream()
                    .map(info -> new TopicPartition(topic, info.partition()))
                    .toList();


            for (TopicPartition tpi : topicPartitions) {
                TopicPartition partition = new TopicPartition(topic, tpi.partition());
                ListOffsetsResult.ListOffsetsResultInfo earliest = Future.call(getAdminClient(clusterId).listOffsets(
                        Collections.singletonMap(partition, OffsetSpec.earliest())).partitionResult(partition));
                ListOffsetsResult.ListOffsetsResultInfo latest = Future.call(getAdminClient(clusterId).listOffsets(
                        Collections.singletonMap(partition, OffsetSpec.latest())).partitionResult(partition));

                totalMessageCount.addAndGet(latest.offset() - earliest.offset());
            }
        }
        return totalMessageCount.get();
    }

    public long getLatestOffset(String clusterId, TopicPartition tp) {
        return Future.call(getAdminClient(clusterId)
                        .listOffsets(Collections.singletonMap(tp, OffsetSpec.latest()))
                        .partitionResult(tp))
                .offset();
    }

    public List<TopicPartition> getTopicPartitions(String topic, KafkaConsumer<String, String> consumer) {
        List<TopicPartition> partitions = new ArrayList<>();
        for (PartitionInfo partitionNumber : consumer.partitionsFor(topic)) {
            partitions.add(new TopicPartition(topic, partitionNumber.partition()));
        }
        return partitions;
    }

    public void assignPartitionsToConsumer(KafkaConsumer<String, String> consumer, List<TopicPartition> partitions) {
        consumer.assign(partitions);
    }

    public void seekConsumerTo(int maxMessagesToFetch, KafkaConsumer<String, String> consumer, List<TopicPartition> partitions) {
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(partitions);

        for (TopicPartition partition : partitions) {
            long currentOffset = endOffsets.get(partition);
            long targetOffset = Math.max(currentOffset - maxMessagesToFetch, 0);  // Prevent negative offset
            consumer.seek(partition, targetOffset);  // Seek to the target offset
        }
    }

    public void seekConsumerAllPartitionsToEnd(String topic, KafkaConsumer<String, String> consumer) {
        consumer.subscribe(Collections.singletonList(topic));
        consumer.poll(Duration.ofMillis(100)); // Initial poll to assign partitions
        for (TopicPartition tp : consumer.assignment()) {
            consumer.seekToEnd(Collections.singletonList(tp));
        }
    }

    public void seekConsumerSpecificPartitionAndOffset(Optional<Long> offsetOpt, KafkaConsumer<String, String> consumer, TopicPartition topicPartition) {
        offsetOpt.ifPresent(offset -> consumer.seek(topicPartition, offset));
    }

    public void seekConsumerSpecificPartitionToEnd(KafkaConsumer<String, String> consumer, TopicPartition topicPartition) {
        consumer.seekToEnd(Collections.singletonList(topicPartition));
    }

    @Cacheable(cacheNames = CacheManagerConfig.CacheOwnerKey.TOPIC_ALL, key = "{#root.methodName,#clusterId,#hideInternal}", unless = "#result == null")
    public List<String> getAllTopics(String clusterId, boolean hideInternal) {
        return Future.call(
                        getAdminClient(clusterId)
                                .listTopics(new ListTopicsOptions().listInternal(hideInternal))
                                .names()
                )
                .stream().toList();
    }

    public ListOffsetsResult.ListOffsetsResultInfo getOffsetForPartition(String clusterId, TopicPartition topicPartition, OffsetSpec spec) throws ExecutionException, InterruptedException {
        return Future.call(
                getAdminClient(clusterId)
                        .listOffsets(Collections.singletonMap(topicPartition, spec))
                        .partitionResult(topicPartition)
        );
    }

    public ListOffsetsResult.ListOffsetsResultInfo getOffsetForTimestamp(String clusterId, TopicPartition topicPartition, long timestamp) throws ExecutionException, InterruptedException {
        return Future.call(getAdminClient(clusterId)
                .listOffsets(Collections.singletonMap(topicPartition, OffsetSpec.forTimestamp(timestamp)))
                .partitionResult(topicPartition));
    }

    public List<TopicPartitionInfo> getPartitionsOfTopic(String clusterId, String topic) {
        return Future.call(
                        getAdminClient(clusterId)
                                .describeTopics(Collections.singletonList(topic))
                                .topicNameValues().get(topic)
                )
                .partitions();
    }


    public long getCurrentOffsetOfPartition(String clusterId, String groupId, TopicPartition topicPartition) {
        Map<TopicPartition, OffsetAndMetadata> currentOffsets = Future.call(
                getAdminClient(clusterId)
                        .listConsumerGroupOffsets(groupId)
                        .partitionsToOffsetAndMetadata()
        );
        OffsetAndMetadata currentOffset = currentOffsets.get(topicPartition);
        return currentOffset != null ? currentOffset.offset() : 0;
    }

    public void changeOffsetOfConsumerGroup(String groupId, String clusterId, TopicPartition topicPartition, long targetOffset) {
        Future.call(
                getAdminClient(clusterId)
                        .alterConsumerGroupOffsets(groupId, Collections.singletonMap(topicPartition, new OffsetAndMetadata(targetOffset)))
                        .all()
        );
    }

    public TopicDescription describeTopic(String clusterId, String topicName) {
        return Future.call(
                getAdminClient(clusterId)
                        .describeTopics(Collections.singletonList(topicName))
                        .topicNameValues()
                        .get(topicName)
        );
    }

    @Cacheable(cacheNames = CacheManagerConfig.CacheOwnerKey.CONSUMER_GROUPS_DESCRIBE, key = "{#root.methodName,#clusterId}", unless = "#result == null")
    public Map<String, ConsumerGroupDescription> describeConsumerGroups(String clusterId) {
        List<String> groupIds = listConsumerGroupIds(clusterId);
        return Future.call(getAdminClient(clusterId).
                describeConsumerGroups(groupIds).all());
    }
}
