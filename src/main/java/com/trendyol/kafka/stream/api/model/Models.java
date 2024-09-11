package com.trendyol.kafka.stream.api.model;

import com.trendyol.kafka.stream.api.service.manager.ConsumerPool;
import lombok.Builder;
import org.apache.kafka.clients.admin.AdminClient;

import java.util.List;
import java.util.Map;

public class Models {
    @Builder
    public record MessageInfo(String key, String topic, int partition, long offset, double sizeInMb,
                              double sizeInKB, double sizeInBytes, String value, Map<String, String> headers,
                              long timestamp, String timestampType) {}

    @Builder
    public record ConsumerGroupInfo(
            String groupId,
            ConsumerGroupCoordinator coordinator,
            String state,
            String partitionAssignor,
            Map<String, List<ConsumerGroupMember>> membersByTopic,
            int podCount,
            int memberCount,
            int assignedTopicCount,
            int assignedPartitionsCount,
            long totalLag
    ) {
    }

    @Builder
    public record ConsumerGroupMember(
            String memberId,
            String clientId,
            String consumerId,
            String host,
            String topic,
            int partition,
            long committedOffset,
            long latestOffset,
            long lag
    ) {}

    @Builder
    public record ConsumerGroupCoordinator(int id,String idString,String host,int port,String rack){}

    @Builder
    public record ClusterAdminInfo(AdminClient adminClient, ConsumerPool consumerPool) {
    }

    public enum OffsetSeek {
        BEGINNING, END, SHIFTBY, DATE
    }

    public record PaginatedResponse<T>(
            List<T> content,
            int currentPage,
            int totalItems,
            int totalPages
    ) {
    }

    @Builder
    public record TopicInfo(
            String name,
            String clusterId,
            Integer partitionCount,
            Long retentionDay,
            Long messageCount,
            Double messageSize,
            Integer replicas,
            Map<String, Object> config
    ){}
}
