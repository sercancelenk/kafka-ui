package com.trendyol.kafka.stream.api.service;

import com.trendyol.kafka.stream.api.adapters.rest.context.RequestContext;
import com.trendyol.kafka.stream.api.application.ConsumerService;
import com.trendyol.kafka.stream.api.domain.Models;
import com.trendyol.kafka.stream.api.adapters.kafka.manager.KafkaWrapper;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.*;


@ExtendWith(MockitoExtension.class)
class ConsumerServiceTest {

    @InjectMocks
    private ConsumerService serviceUnderTest; // Replace with the class containing extractConsumerGroupInfo

    @Mock
    private ConsumerGroupDescription groupDescription;

    @Mock
    private KafkaWrapper kafkaWrapper;  // Mock KafkaWrapper (or any wrapper class you use)

    @Mock
    private MemberDescription member;

    @Mock
    private OffsetAndMetadata offsetAndMetadata;

    @Mock
    private Node coordinatorNode;

    @Mock
    private MemberAssignment assignment;

    @Mock
    private TopicPartition topicPartition;

    @BeforeEach
    public void setUp() {
        // Mock group description
        when(groupDescription.groupId()).thenReturn("test-group");
        when(groupDescription.coordinator()).thenReturn(coordinatorNode);
        when(groupDescription.state()).thenReturn(ConsumerGroupState.STABLE);  // Replace with appropriate state enum

        // Mock coordinator node details
        when(coordinatorNode.id()).thenReturn(1);
        when(coordinatorNode.idString()).thenReturn("1");
        when(coordinatorNode.host()).thenReturn("localhost");
        when(coordinatorNode.port()).thenReturn(9092);
        when(coordinatorNode.rack()).thenReturn("rack1");

        // Mock a consumer group member
        when(member.consumerId()).thenReturn("consumer1");
        when(member.clientId()).thenReturn("client1");
        when(member.host()).thenReturn("127.0.0.1");
        when(member.assignment()).thenReturn(assignment);

        // Mock member assignments (topic partitions)
        when(assignment.topicPartitions()).thenReturn(Collections.singleton(topicPartition));
        when(topicPartition.topic()).thenReturn("test-topic");
        when(topicPartition.partition()).thenReturn(0);

        // Mock committed offset and latest offset for topic partition
        when(kafkaWrapper.getCommittedOffset(anyString(), anyString(), eq(topicPartition)))
                .thenReturn(offsetAndMetadata);
        when(kafkaWrapper.getLatestOffset(anyString(), eq(topicPartition)))
                .thenReturn(100L);
        when(offsetAndMetadata.offset()).thenReturn(90L);

        // Mock the list of members in the group
        when(groupDescription.members()).thenReturn(Collections.singleton(member));
    }

    @Test
    public void testExtractConsumerGroupInfo() {
        // Call the method under test
        try (MockedStatic<RequestContext> mockedStatic = mockStatic(RequestContext.class)) {
            // Mock the behavior to return a non-null value for the header
            mockedStatic.when(RequestContext::getClusterId)
                    .thenReturn("some-cluster-id");


            Models.ConsumerGroupInfo result = serviceUnderTest.extractConsumerGroupInfo(groupDescription);

            // Assertions to verify the result
            assertNotNull(result);
            assertEquals("test-group", result.groupId());
            assertEquals("Stable", result.state());
            assertEquals(1, result.memberCount());
            assertEquals(1, result.assignedPartitionsCount());
            assertEquals(10L, result.totalLag());  // latestOffset (100) - committedOffset (90) = 10
            assertEquals(1, result.podCount());
            assertEquals("rack1", result.coordinator().rack());

            // Verify that the KafkaWrapper was called correctly
            verify(kafkaWrapper, times(1)).getCommittedOffset(anyString(), anyString(), eq(topicPartition));
            verify(kafkaWrapper, times(1)).getLatestOffset(anyString(), eq(topicPartition));
        }
    }

}