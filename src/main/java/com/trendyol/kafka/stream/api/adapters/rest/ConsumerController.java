package com.trendyol.kafka.stream.api.adapters.rest;

import com.trendyol.kafka.stream.api.domain.Models;
import com.trendyol.kafka.stream.api.application.ConsumerService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.Parameter;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.concurrent.ExecutionException;

@RestController
@RequiredArgsConstructor
@RequestMapping("consumers")
public class ConsumerController {
    private final ConsumerService consumerService; //Constructor injection

    @GetMapping("/consumer-group-info-by-topic")
    public List<Models.ConsumerGroupInfo> getConsumerGroupInfoByTopic(
            @RequestHeader("x-cluster-id") String clusterId,
            @RequestParam String topic) {
        return consumerService.getConsumerGroupInfoByTopic(clusterId, topic);
    }

    @GetMapping("/consumer-group-info-by-group-id")
    public List<Models.ConsumerGroupInfo> getConsumerGroupInfoByGroupId(
            @RequestHeader("x-cluster-id") String clusterId,
            @RequestParam String groupId) throws ExecutionException, InterruptedException {
        return consumerService.getConsumerGroupInfoByGroupId(clusterId, groupId);
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
    public ResponseEntity<Models.PaginatedResponse<String>> getConsumerGroups(
            @RequestHeader("x-cluster-id") String clusterId,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size) {
        Models.PaginatedResponse<String> consumerGroups = consumerService.getConsumerGroupIdsPaginated(clusterId, page, size);
        return ResponseEntity.ok(consumerGroups);
    }

}
