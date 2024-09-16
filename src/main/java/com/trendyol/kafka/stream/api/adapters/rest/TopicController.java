package com.trendyol.kafka.stream.api.adapters.rest;

import com.trendyol.kafka.stream.api.domain.Models;
import com.trendyol.kafka.stream.api.application.TopicService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

@RestController
@RequestMapping("topics")
@Slf4j
public class TopicController {
    @Autowired
    private TopicService kafkaTopMessagesService;

    @GetMapping("/top-latest-messages")
    public List<Models.MessageInfo> getTopLatestMessages(
            @RequestHeader("x-cluster-id") String clusterId,
            @RequestParam(defaultValue = "1") int maxMessagesToFetch,  // Allow dynamic message fetching
            @RequestParam(defaultValue = "your-kafka-topic") String topic) throws InterruptedException {
        return kafkaTopMessagesService.getTopMessagesFromTopic(clusterId, topic, maxMessagesToFetch);
    }

    @GetMapping("/message")
    public Models.MessageInfo getMessage(
            @RequestHeader("x-cluster-id") String clusterId,
            @RequestParam String topic,
            @RequestParam Optional<Integer> partition,
            @RequestParam Optional<Long> offset) throws InterruptedException {
        return kafkaTopMessagesService.getOneMessageFromTopic(clusterId, topic, partition, offset);
    }

    @GetMapping("/topic-info")
    public Models.TopicInfo getTopicInfo(
            @RequestHeader("x-cluster-id") String clusterId,
            @RequestParam String topicName) throws ExecutionException, InterruptedException {
        return kafkaTopMessagesService.getTopicInfo(clusterId, topicName);
    }

    @GetMapping("/lag")
    public Map<String, Long> getLagCount(
            @RequestHeader("x-cluster-id") String clusterId,
            @RequestParam String topicName,
            @RequestParam String groupId) throws ExecutionException, InterruptedException {
        return kafkaTopMessagesService.getLagCount(clusterId, topicName, groupId);
    }


    @GetMapping
    public Models.PaginatedResponse<String> getPaginatedTopics(
            @RequestHeader("x-cluster-id") String clusterId,
            @Parameter(description = "Page number, starting from 0", example = "0")
            @RequestParam(defaultValue = "0") int page,

            @Parameter(description = "Size of the page (number of topics per page)", example = "10")
            @RequestParam(defaultValue = "10") int size
    ) throws ExecutionException, InterruptedException {
        return kafkaTopMessagesService.getTopicListUnCached(clusterId, page, size);
    }
}
