package com.trendyol.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;

import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@SpringBootApplication
@Slf4j
public class JKafkaStreamApiApplication {

    public static void main(String[] args) {
        SpringApplication.run(JKafkaStreamApiApplication.class, args);
    }

    @Bean
    public CommandLineRunner runner(ApplicationContext applicationContext){
        return args -> {
//            log.info("Commandlinerunner is fired");
//            String[] beanNames = applicationContext.getBeanDefinitionNames();
//            Arrays.sort(beanNames); // Sort bean names alphabetically for easier reading
//
//            log.info("Listing all Spring Boot beans:");
//
//            for (String beanName : beanNames) {
//                log.info(beanName);
//            }
        };

    }

    public static void produceMessages(){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Kafka broker
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the Kafka Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        String topic = "demo.topic.2";  // Replace with your topic name

        // Produce 1 million records
        int messageCount = 10_000_000;
        long startTime = System.currentTimeMillis();  // Start time for performance tracking
        Function<Integer, Integer> generateRandomNumber = (max) -> {
            Random random = new Random();
            // nextInt(4) generates a number between 0 and 3, so we add 1 to get a number between 1 and 4
            return random.nextInt(max) + 1;
        };
        for (int i = 1; i <= messageCount; i++) {
            String key = "key-" + i;
            String value = "message-" + i;
            String t = topic;//topic.concat(generateRandomNumber.apply(4).toString());
            // Create a producer record
            ProducerRecord<String, String> record = new ProducerRecord<>(t, key, value);

            // Send the record (asynchronously)
            Future<RecordMetadata> future = producer.send(record);

            // Optionally, block until the message is acknowledged (synchronously)
            // future.get(); // Uncomment this line if you want synchronous sending
        }

        // Calculate and print the total time taken
        long endTime = System.currentTimeMillis();
        System.out.println("Produced " + messageCount + " records in " + (endTime - startTime) + " ms.");

        // Close the producer
        producer.close();
    }

    public static void deleteAllTopics() throws ExecutionException, InterruptedException {
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"); // Kafka broker address

        try (AdminClient adminClient = AdminClient.create(properties)) {
            // Define the wildcard pattern (e.g., delete all topics starting with "test-")
            Pattern pattern = Pattern.compile("demo.topic*");

            // List all topics
            Set<String> allTopics = adminClient.listTopics().names().get();

            // Filter topics based on the wildcard pattern
            List<String> topicsToDelete = allTopics.stream()
//                        .filter(topic -> pattern.matcher(topic).matches())
                    .collect(Collectors.toList());

            // Delete the matching topics
            if (!topicsToDelete.isEmpty()) {
                System.out.println("Deleting topics: " + topicsToDelete);
                DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(topicsToDelete);
                deleteTopicsResult.all().get(); // Wait for the deletion to complete
                System.out.println("Topics deleted successfully.");
            } else {
                System.out.println("No topics matched the pattern.");
            }
        }
    }
}