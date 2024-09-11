package com.trendyol.kafka.stream.api.service.manager;

import com.trendyol.kafka.stream.api.controller.context.RequestContext;
import com.trendyol.kafka.stream.api.model.Exceptions;
import com.trendyol.kafka.stream.api.model.Models;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
@RequiredArgsConstructor
public class KafkaWrapper {
    private final Map<String, Models.ClusterAdminInfo> clusterAdminManager;
    private final Long timeoutSeconds = 20L;

    public Models.ClusterAdminInfo getClusterAdmin(String clusterId) {
        if (clusterAdminManager.containsKey(clusterId)) {
            return clusterAdminManager.get(clusterId);
        }
        throw Exceptions.ClusterNotSupportedException.builder().params(new Object[]{clusterId}).build();
    }

    public ConsumerGroupDescription getSingleConsumerGroupDescription(String clusterId, String groupId) {
        try {
            return getClusterAdmin(clusterId).adminClient()
                    .describeConsumerGroups(Collections.singletonList(groupId)).
                    describedGroups().get(groupId).get();
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
            return getClusterAdmin(clusterId).adminClient()
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
        try {
            return getClusterAdmin(clusterId).adminClient().listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata().get()
                    .entrySet().stream()
                    .anyMatch(entry -> entry.getKey().topic().equals(topic));
        } catch (InterruptedException | ExecutionException e) {
            throw new Exceptions.ProcessExecutionException(e);
        }
    }

    public OffsetAndMetadata getCommittedOffset(String clusterId, String groupId, TopicPartition tp) {
        try {
            return getClusterAdmin(clusterId).adminClient()
                    .listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata()
                    .get()
                    .get(tp);
        } catch (InterruptedException | ExecutionException e) {
            throw new Exceptions.ProcessExecutionException(e);
        }
    }

    public long getLatestOffset(String clusterId, TopicPartition tp) {
        try {
            return getClusterAdmin(clusterId).adminClient()
                    .listOffsets(Collections.singletonMap(tp, OffsetSpec.latest()))
                    .partitionResult(tp)
                    .get()
                    .offset();
        } catch (InterruptedException | ExecutionException e) {
            throw new Exceptions.ProcessExecutionException(e);
        }
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

    public List<String> getAllTopics(String clusterId, boolean hideInternal) {
        try {
            return getClusterAdmin(clusterId).adminClient()
                    .listTopics(new ListTopicsOptions().listInternal(hideInternal))
                    .names()
                    .get().stream().toList();
        } catch (InterruptedException | ExecutionException e) {
            throw new Exceptions.ProcessExecutionException(e);
        }
    }

    public ListOffsetsResult.ListOffsetsResultInfo getOffsetForPartition(String clusterId, TopicPartition topicPartition, OffsetSpec spec) throws ExecutionException, InterruptedException {
        ListOffsetsResult result = getClusterAdmin(clusterId).adminClient()
                .listOffsets(Collections.singletonMap(topicPartition, spec));
        return result.partitionResult(topicPartition).get();
    }

    public ListOffsetsResult.ListOffsetsResultInfo getOffsetForTimestamp(String clusterId, TopicPartition topicPartition, long timestamp) throws ExecutionException, InterruptedException {
        ListOffsetsResult result = getClusterAdmin(clusterId).adminClient()
                .listOffsets(Collections.singletonMap(topicPartition, OffsetSpec.forTimestamp(timestamp)));
        return result.partitionResult(topicPartition).get();
    }

    public List<TopicPartitionInfo> getPartitionsOfTopic(String clusterId, String topic) {
        try {
            return getClusterAdmin(clusterId).adminClient()
                    .describeTopics(Collections.singletonList(topic))
                    .topicNameValues().get(topic).get()
                    .partitions();
        } catch (InterruptedException | ExecutionException e) {
            throw new Exceptions.ProcessExecutionException(e);
        }
    }

    public long getCurrentOffsetOfPartition(String clusterId, String groupId, TopicPartition topicPartition) {
        try {
            Map<TopicPartition, OffsetAndMetadata> currentOffsets = getClusterAdmin(clusterId).adminClient()
                    .listConsumerGroupOffsets(groupId)
                    .partitionsToOffsetAndMetadata()
                    .get(timeoutSeconds, TimeUnit.SECONDS);

            OffsetAndMetadata currentOffset = currentOffsets.get(topicPartition);
            return currentOffset != null ? currentOffset.offset() : 0;
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new Exceptions.ProcessExecutionException(e);
        }
    }

    public void changeOffsetOfConsumerGroup(String groupId, String clusterId, TopicPartition topicPartition, long targetOffset) {
        try {
            getClusterAdmin(clusterId).adminClient()
                    .alterConsumerGroupOffsets(groupId, Collections.singletonMap(topicPartition, new OffsetAndMetadata(targetOffset)))
                    .all().get(timeoutSeconds, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw new Exceptions.ProcessExecutionException(e);
        }
    }

    public TopicDescription describeTopic(String clusterId, String topicName) {
        try {
            return getClusterAdmin(clusterId).adminClient()
                    .describeTopics(Collections.singletonList(topicName))
                    .topicNameValues()
                    .get(topicName)
                    .get();
        } catch (InterruptedException | ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof org.apache.kafka.common.errors.UnknownTopicOrPartitionException) {
                throw Exceptions.TopicNotFoundException.builder()
                        .params(new Object[]{topicName, RequestContext.getClusterId()}).build();
            }
            throw new Exceptions.ProcessExecutionException(e);
        }

    }
}
