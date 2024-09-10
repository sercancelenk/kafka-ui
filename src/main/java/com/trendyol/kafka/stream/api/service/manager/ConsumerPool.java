package com.trendyol.kafka.stream.api.service.manager;

import com.trendyol.kafka.stream.api.controller.context.RequestContext;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@Slf4j
public class ConsumerPool {

    private final BlockingQueue<KafkaConsumer<String, String>> cp;
    private final String clusterId;
    private final ClusterInfoConfig.ClusterInfo clusterInfo;

    public ConsumerPool(String clusterId, ClusterInfoConfig.ClusterInfo clusterInfo) {
        this.clusterId = clusterId;
        this.clusterInfo = clusterInfo;

        cp = new LinkedBlockingQueue<>(clusterInfo.consumeSettings().consumerPoolSize());
        initializeConsumerPool(clusterInfo.consumeSettings().consumerPoolSize());
    }

    private void initializeConsumerPool(int poolSize) {
        for (int i = 0; i < poolSize; i++) {
            KafkaConsumer<String, String> consumer = createConsumer();
            boolean offer = cp.offer(consumer);// Add the consumer to the pool
            if (offer) log.debug("Consumer is added to pool for cluster {}", clusterId);
        }
    }

    // Method to create a KafkaConsumer without group.id
    private KafkaConsumer<String, String> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInfo.brokers());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, clusterInfo.consumeSettings().offsetReset());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, clusterInfo.consumeSettings().autoCommit());
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, clusterInfo.consumeSettings().sessionTimeout());
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, clusterInfo.consumeSettings().clientId());
        props.put("connections.max.idle.ms", clusterInfo.consumeSettings().connectionsMaxIdleMs());
        Optional.ofNullable(clusterInfo.secureSettings())
                .ifPresent(props::putAll);
        return new KafkaConsumer<>(props);
    }

    public KafkaConsumer<String, String> borrowConsumer() throws InterruptedException {
        log.debug("Borrowing consumer for cluster {}", RequestContext.getClusterId());
        return cp.take();  // Block until a consumer is available
    }

    public void returnConsumer(KafkaConsumer<String, String> consumer) {
        consumer.unsubscribe();
        boolean offer = cp.offer(consumer);
        if (offer) log.debug("Releasing and unsubscribing consumer to pool is success for cluster " + RequestContext.getClusterId());
    }

    public void stopAll() {
        for (KafkaConsumer<String, String> consumer : cp) {
            consumer.close();
            log.info("Consumer stopped. {}", consumer);
        }
    }
}