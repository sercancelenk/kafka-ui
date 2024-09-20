package com.trendyol.kafka.stream.api.adapters.kafka.manager;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@Service
@RequiredArgsConstructor
@Slf4j
public class AdminClientService {
    public AdminClient create(ClusterInfoConfig.ClusterInfo info) {
        Map<String, Object> props = new HashMap<>();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, info.brokers());

        Optional.ofNullable(info.adminClientSettings())
                .ifPresent(props::putAll);

        Optional.ofNullable(info.secureSettings())
                .ifPresent(props::putAll);

        return AdminClient.create(props);
    }
}
