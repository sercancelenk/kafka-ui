package com.trendyol.kafka.stream.api.model;

import com.trendyol.kafka.stream.api.service.manager.ConsumerPool;
import lombok.Builder;
import org.apache.kafka.clients.admin.AdminClient;

import java.util.List;

public class Models {
    @Builder
    public record MessageInfo(
            String topic,
            int partition,
            long offset,
            int size,
            String value
    ) {
    }

    @Builder
    public record ConsumerGroupInfo(
            String groupId,
            String state,
            String partitionAssignor,
            List<ConsumerGroupMember> members,
            long totalCommitted,
            long totalLatest,
            long totalLag,
            int podCount,
            int memberCount
    ) {
    }

    @Builder
    public record ConsumerGroupMember(
            String memberId,
            String clientId,
            String host,
            String topic,
            int partition,
            long committedOffset,
            long latestOffset,
            long lag
    ) {
    }

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

}
