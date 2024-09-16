package com.trendyol.kafka.stream.api.application;

import com.trendyol.kafka.stream.api.adapters.kafka.manager.KafkaWrapper;
import com.trendyol.kafka.stream.api.domain.Exceptions;
import com.trendyol.kafka.stream.api.domain.Models;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.TopicPartitionInfo;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.ExecutionException;

@Service
@RequiredArgsConstructor
public class OperationService {
    private final KafkaWrapper kafkaWrapper;

    public void changeConsumerGroupOffset(String clusterId, String groupId, String topic, Models.OffsetSeek option, Long value) throws ExecutionException, InterruptedException {
        List<TopicPartitionInfo> partitions = kafkaWrapper.getPartitionsOfTopic(clusterId, topic);

        for (TopicPartitionInfo partitionInfo : partitions) {
            TopicPartition topicPartition = new TopicPartition(topic, partitionInfo.partition());
            long targetOffset = switch (option) {
                case BEGINNING ->
                        kafkaWrapper.getOffsetForPartition(clusterId, topicPartition, OffsetSpec.earliest()).offset();
                case END -> kafkaWrapper.getOffsetForPartition(clusterId, topicPartition, OffsetSpec.latest()).offset();
                case DATE -> {
                    boolean valid = validateTimestamp(clusterId, topic, value);
                    if (valid)
                        yield kafkaWrapper.getOffsetForTimestamp(clusterId, topicPartition, value).offset();
                    else throw Exceptions.ChangeOffsetTimestampNotAcceptable.builder().build();
                }
                default -> throw new IllegalArgumentException("Unknown option: " + option);
            };

            kafkaWrapper.changeOffsetOfConsumerGroup(groupId, clusterId, topicPartition, targetOffset);
        }
    }

    public boolean validateTimestamp(String clusterId, String topic, long timestamp) throws ExecutionException, InterruptedException {
        List<TopicPartitionInfo> partitions = kafkaWrapper.getPartitionsOfTopic(clusterId, topic);
        long earliestTimestamp = Long.MAX_VALUE;
        long latestTimestamp = Long.MIN_VALUE;

        for (TopicPartitionInfo partitionInfo : partitions) {
            TopicPartition topicPartition = new TopicPartition(topic, partitionInfo.partition());

            // Retrieve earliest and latest offsets
            long partitionEarliestTimestamp = kafkaWrapper.getOffsetForPartition(clusterId, topicPartition, OffsetSpec.earliest()).timestamp();
            long partitionLatestTimestamp = kafkaWrapper.getOffsetForPartition(clusterId, topicPartition, OffsetSpec.latest()).timestamp();

            // Update the overall earliest and latest timestamps
            if (partitionEarliestTimestamp < earliestTimestamp) {
                earliestTimestamp = partitionEarliestTimestamp;
            }
            if (partitionLatestTimestamp > latestTimestamp) {
                latestTimestamp = partitionLatestTimestamp;
            }
        }

        // Validate the provided timestamp
        return (timestamp < earliestTimestamp) || (timestamp > latestTimestamp);
    }

}
