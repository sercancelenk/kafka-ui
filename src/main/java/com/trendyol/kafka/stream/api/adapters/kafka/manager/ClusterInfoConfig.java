package com.trendyol.kafka.stream.api.adapters.kafka.manager;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "kafka")
@Data
public class ClusterInfoConfig {

    public record AdminClientSetting(
            String requestTimeoutMs,
            String connectionsMaxIdleMs,
            String socketConnectionTimeoutMs,
            String retries,
            String retryBackoffMs
    ){}

    public record ConsumeSetting
            (String offsetReset,
             Integer sessionTimeout,
             String clientId,
             Boolean autoCommit,
             Integer consumerPoolSize,
             String connectionsMaxIdleMs) {
    }

    public record SecureCluster
            (String jaasUser,
             String jaasPass,
             String saslMechanism,
             String saslJaasConfig,
             String sslTruststoreLocation,
             String sslEndpointIdentityAlgorithm,
             String securityProtocol,
             String sslTruststorePassword) {
    }

    public record ClusterInfo
            (Boolean enable, String id,
             String brokers,
             ConsumeSetting consumeSettings,
             Map<String, Object> adminClientSettings,
             Map<String, String> secureSettings) {
    }

    Map<String, ClusterInfo> clusters;
}


