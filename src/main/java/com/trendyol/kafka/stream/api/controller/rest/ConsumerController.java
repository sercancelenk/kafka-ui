package com.trendyol.kafka.stream.api.controller.rest;

import com.trendyol.kafka.stream.api.model.Exceptions;
import com.trendyol.kafka.stream.api.model.Models;
import com.trendyol.kafka.stream.api.service.ConsumerService;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

@RestController
@RequiredArgsConstructor
@RequestMapping("consumers")
public class ConsumerController {
    private final ConsumerService consumerService; //Constructor injection

    @Operation(summary = "Get Consumer Group Info by Topic", description = "Retrieve a list of consumer groups that are consuming from the specified Kafka topic.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved consumer groups",
                    content = @Content(mediaType = "application/json",
                            schema = @Schema(implementation = Models.ConsumerGroupInfo.class))),
            @ApiResponse(responseCode = "400", description = "Invalid input provided", content = @Content),
            @ApiResponse(responseCode = "500", description = "Internal server error", content = @Content)
    })
    @GetMapping("/consumer-group-info-by-topic")
    public List<Models.ConsumerGroupInfo> getConsumerGroupInfoByTopic(
            @RequestParam String topic) throws ExecutionException, InterruptedException {
        return consumerService.getConsumerGroupInfoByTopic(topic);
    }

    @Operation(summary = "Get Consumer Group Info by Group ID", description = "Retrieve detailed information for a specific Kafka consumer group based on its group ID.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successfully retrieved consumer group info",
                    content = @Content(mediaType = "application/json",
                            schema = @Schema(implementation = Models.ConsumerGroupInfo.class))),
            @ApiResponse(responseCode = "400", description = "Invalid input provided", content = @Content),
            @ApiResponse(responseCode = "500", description = "Internal server error", content = @Content)
    })
    @GetMapping("/consumer-group-info-by-group-id")
    public List<Models.ConsumerGroupInfo> getConsumerGroupInfoByGroupId(
            @RequestParam String groupId) throws ExecutionException, InterruptedException {
        return consumerService.getConsumerGroupInfoByGroupId(groupId);
    }

    @Operation(summary = "Get paginated Kafka consumer group IDs",
            description = "Retrieve paginated consumer group IDs from a Kafka cluster.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Successful retrieval of consumer group IDs",
                    content = @Content(mediaType = "application/json",
                            schema = @Schema(implementation = Models.PaginatedResponse.class))),
            @ApiResponse(responseCode = "500", description = "Internal server error",
                    content = @Content)
    })
    @GetMapping
    @CircuitBreaker(name = "getConsumerGroups", fallbackMethod = "getConsumerGroupsFallback")
    public ResponseEntity<Models.PaginatedResponse<String>> getConsumerGroups(
            @Parameter(description = "Page number (0-based index)", example = "0")
            @RequestParam(defaultValue = "0") int page,

            @Parameter(description = "Number of items per page", example = "10")
            @RequestParam(defaultValue = "10") int size) {
        Models.PaginatedResponse<String> consumerGroups = consumerService.getConsumerGroupIdsPaginated(page, size);
        return ResponseEntity.ok(consumerGroups);
    }


    public ResponseEntity<Models.PaginatedResponse<Object>> getConsumerGroupsFallback(int page, int size, Throwable throwable) {
        System.out.println("Circuit breaker triggered for getConsumerGroups");
        Models.PaginatedResponse<Object> fallbackResponse = new Models.PaginatedResponse<Object>(Collections.emptyList(), page, size, 0);
        return ResponseEntity.ok(fallbackResponse);
    }

}
