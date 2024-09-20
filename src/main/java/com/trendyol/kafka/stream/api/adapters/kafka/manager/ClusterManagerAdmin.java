package com.trendyol.kafka.stream.api.adapters.kafka.manager;

import com.trendyol.kafka.stream.api.domain.Models;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class ClusterManagerAdmin {
    private final ClusterInfoConfig clusterInfoConfig;
    private final AdminClientService adminClientService;
    private final Map<String, Models.ClusterAdminInfo> clusterAdminManagerMap = new ConcurrentHashMap<>();

    @Bean
    public Map<String, Models.ClusterAdminInfo> clusterAdminManager() {
        clusterInfoConfig.getClusters()
                .entrySet()
                .stream()
                .filter(entry -> entry.getValue().enable())
                .forEach(entry -> {
                    ClusterInfoConfig.ClusterInfo info = entry.getValue();
                    String clusterId = info.id();

                    AdminClient adminClient = adminClientService.create(info);
                    ConsumerPool consumerPool = new ConsumerPool(clusterId, info);
                    clusterAdminManagerMap.put(clusterId, new Models.ClusterAdminInfo(adminClient, consumerPool));
                    log.info("ClusterAdminManager is created for {} cluster with AdminClient {} and ConsumerPool {}", clusterId, adminClient, consumerPool);
                });

        return clusterAdminManagerMap;
    }

    @PreDestroy
    public void destroy() {
        clusterAdminManagerMap
                .forEach((clusterId, info) -> {
                    try {
                        info.adminClient().close();
                        log.info("Admin client for cluster {} is stopped", clusterId);
                    } catch (Exception e) {
                        log.error("Error closing AdminClient for cluster {}: {}", clusterId, e.getMessage());
                    }
                    try {
                        info.consumerPool().stopAll();
                        log.info("Consumer pool for cluster {} is stopped", clusterId);
                    } catch (Exception e) {
                        log.error("Error stopping ConsumerPool for cluster {}: {}", clusterId, e.getMessage());
                    }
                });
    }
}