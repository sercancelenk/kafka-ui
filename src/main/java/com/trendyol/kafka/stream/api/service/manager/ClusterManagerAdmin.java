package com.trendyol.kafka.stream.api.service.manager;

import com.trendyol.kafka.stream.api.model.Models;
import jakarta.annotation.PreDestroy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Configuration
@RequiredArgsConstructor
@Slf4j
public class ClusterManagerAdmin {
    private final ClusterInfoConfig clusterInfoConfig;
    private final Map<String, Models.ClusterAdminInfo> clusterAdminManagerMap = new ConcurrentHashMap<>();

    @PreDestroy
    public void destroy() {
        clusterAdminManagerMap
                .forEach((clusterId, info)->{
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

    @Bean
    public Map<String, Models.ClusterAdminInfo> clusterAdminManager() {
        clusterInfoConfig.getClusters()
                .entrySet()
                .stream()
                .filter(entry-> entry.getValue().enable())
                .forEach(entry -> {
                    ClusterInfoConfig.ClusterInfo info = entry.getValue();
                    String clusterId = info.id();

                    AdminClient adminClient = createAdminClient(info);
                    ConsumerPool consumerPool = new ConsumerPool(clusterId, info);
                    clusterAdminManagerMap.put(clusterId, new Models.ClusterAdminInfo(adminClient, consumerPool));
                    log.info("ClusterAdminManager is created for {} cluster with AdminClient {} and ConsumerPool {}", clusterId, adminClient, consumerPool);
                });

        return clusterAdminManagerMap;
    }

    public AdminClient createAdminClient(ClusterInfoConfig.ClusterInfo info) {
        Map<String, Object> props = new HashMap<>();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, info.brokers());

        Optional.ofNullable(info.adminClientSettings())
                        .ifPresent(props::putAll);

        Optional.ofNullable(info.secureSettings())
                .ifPresent(props::putAll);

        return AdminClient.create(props);
    }
}