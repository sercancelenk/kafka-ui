package com.trendyol.kafka.stream.api.controller.rest;

import com.trendyol.kafka.stream.api.model.Models;
import com.trendyol.kafka.stream.api.service.OperationService;
import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("operations")
@Slf4j
@RequiredArgsConstructor
public class OperationController {
    private final OperationService operationService;

    @Operation(summary = "Change Kafka consumer group offset",
            description = "Apply the Kafka consumer group offset change across all partitions of the topic.")
    @ApiResponses(value = {
            @ApiResponse(responseCode = "200", description = "Offset change successful"),
            @ApiResponse(responseCode = "400", description = "Invalid offset change request"),
            @ApiResponse(responseCode = "500", description = "Internal server error")
    })
    @PostMapping("/change-offset")
    public String changeConsumerGroupOffset(
            @RequestParam String groupId,
            @RequestParam String topic,
            @RequestParam String option,
            @RequestParam(required = false) Long value) {
        try {
            operationService.changeConsumerGroupOffset(groupId, topic, Models.OffsetSeek.valueOf(option), value);
            return "Offset change applied successfully.";
        } catch (Exception e) {
            return "Internal server error: " + e.getMessage();
        }
    }
}
