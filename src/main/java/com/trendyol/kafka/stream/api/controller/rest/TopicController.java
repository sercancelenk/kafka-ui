package com.trendyol.kafka.stream.api.controller.rest;

import com.trendyol.kafka.stream.api.model.Models;
import com.trendyol.kafka.stream.api.service.TopicService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

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

    @Operation(summary = "Get the top latest Kafka messages", description = "Fetch a list of the top latest messages from a specified Kafka topic. You can limit the number of messages to fetch.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved messages",
                    content = @Content(mediaType = "application/json",
                            schema = @Schema(implementation = Models.MessageInfo.class))),
            @ApiResponse(responseCode = "400", description = "Invalid input provided", content = @Content),
            @ApiResponse(responseCode = "500", description = "Internal server error", content = @Content)
    })
    @GetMapping("/top-latest-messages")
    public List<Models.MessageInfo> getTopLatestMessages(
            @RequestParam(defaultValue = "1") int maxMessagesToFetch,  // Allow dynamic message fetching
            @RequestParam(defaultValue = "your-kafka-topic") String topic) throws InterruptedException {
        return kafkaTopMessagesService.getTopMessagesFromTopic(topic, maxMessagesToFetch);
    }

    @Operation(summary = "Get a message from Kafka topic", description = "Fetch a message from a specific Kafka topic. Optionally provide partition and offset to fetch a specific message. If no partition or offset is provided, the latest message is returned.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Message retrieved successfully",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = Models.MessageInfo.class))}),
            @ApiResponse(responseCode = "400", description = "Invalid input",
                    content = @Content)
    })
    @GetMapping("/message")
    public Models.MessageInfo getMessage(
            @RequestParam String topic,
            @RequestParam Optional<Integer> partition,
            @RequestParam Optional<Long> offset) throws InterruptedException {
        return kafkaTopMessagesService.getOneMessageFromTopic(topic, partition, offset);
    }

    @Operation(summary = "Get topic information", description = "Fetch information like partition count, retention period, and message count for a specific Kafka topic in a given cluster.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved topic information",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = Models.TopicInfo.class))}),
            @ApiResponse(responseCode = "400", description = "Invalid request parameters", content = @Content),
            @ApiResponse(responseCode = "500", description = "Internal server error", content = @Content)
    })
    @GetMapping("/topic-info")
    public Models.TopicInfo getTopicInfo(
            @Parameter(description = "The Kafka topic name to fetch information about", example = "my-topic")
            @RequestParam String topicName) throws ExecutionException, InterruptedException {

        return kafkaTopMessagesService.getTopicInfo(topicName);
    }

    @Operation(
            summary = "Get Kafka Consumer Group Lag",
            description = "Fetch the lag for a specific consumer group in a Kafka cluster based on the topic name and group ID."
    )
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved lag information",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = Map.class))}),
            @ApiResponse(responseCode = "400", description = "Invalid request parameters", content = @Content),
            @ApiResponse(responseCode = "500", description = "Internal server error", content = @Content)
    })
    @GetMapping("/lag")
    public Map<String, Long> getLagCount(
            @RequestParam String topicName,
            @RequestParam String groupId) throws ExecutionException, InterruptedException {
        return kafkaTopMessagesService.getLagCount(topicName, groupId);
    }


    @Operation(summary = "Get paginated Kafka topics", description = "Fetches a paginated list of Kafka topics, excluding internal topics.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved topics",
                    content = {@Content(mediaType = "application/json",
                            schema = @Schema(implementation = Models.PaginatedResponse.class))}),
            @ApiResponse(responseCode = "400", description = "Invalid request parameters", content = @Content),
            @ApiResponse(responseCode = "500", description = "Internal server error", content = @Content)
    })
    @GetMapping
    public Models.PaginatedResponse<String> getPaginatedTopics(
            @Parameter(description = "Page number, starting from 0", example = "0")
            @RequestParam(defaultValue = "0") int page,

            @Parameter(description = "Size of the page (number of topics per page)", example = "10")
            @RequestParam(defaultValue = "10") int size
    ) throws ExecutionException, InterruptedException {
        log.info("Incomint list topics request");
        return kafkaTopMessagesService.getTopicListUnCached(page, size);
    }
}
