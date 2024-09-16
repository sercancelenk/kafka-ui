package com.trendyol.kafka.stream.api.adapters.rest;

import com.trendyol.kafka.stream.api.domain.Models;
import com.trendyol.kafka.stream.api.application.OperationService;
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

    @PostMapping("/change-offset")
    public String changeConsumerGroupOffset(
            @RequestHeader("x-cluster-id") String clusterId,
            @RequestParam String groupId,
            @RequestParam String topic,
            @RequestParam String option,
            @RequestParam(required = false) Long value) {
        try {
            operationService.changeConsumerGroupOffset(clusterId, groupId, topic, Models.OffsetSeek.valueOf(option), value);
            return "Offset change applied successfully.";
        } catch (Exception e) {
            return "Internal server error: " + e.getMessage();
        }
    }
}
