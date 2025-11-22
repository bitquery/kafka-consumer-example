package com.kafkaconsumer;

import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaConsumerApp {
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerApp.class);
    
    private static String extractCertFromResources(String resourcePath) throws Exception {
        ClassLoader classLoader = KafkaConsumerApp.class.getClassLoader();
        InputStream inputStream = classLoader.getResourceAsStream(resourcePath);
        
        if (inputStream == null) {
            throw new Exception("Certificate resource not found: " + resourcePath);
        }
        
        File tempDir = new File(System.getProperty("java.io.tmpdir"), "kafka-certs-" + System.currentTimeMillis());
        tempDir.mkdirs();
        
        File tempFile = new File(tempDir, new File(resourcePath).getName());
        
        try (FileOutputStream fos = new FileOutputStream(tempFile)) {
            byte[] buffer = new byte[1024];
            int length;
            while ((length = inputStream.read(buffer)) > 0) {
                fos.write(buffer, 0, length);
            }
        }
        
        logger.info("Extracted certificate from resources: {} -> {}", resourcePath, tempFile.getAbsolutePath());
        return tempFile.getAbsolutePath();
    }
    
    public static void main(String[] args) {
        String bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        String username = System.getenv("KAFKA_USERNAME");
        String password = System.getenv("KAFKA_PASSWORD");
        String topic = System.getenv("KAFKA_TOPIC");
        String groupId = System.getenv().getOrDefault("KAFKA_GROUP_ID", "kafka-consumer-group");
        String securityProtocol = System.getenv().getOrDefault("KAFKA_SECURITY_PROTOCOL", "SASL_SSL");
        
        if (bootstrapServers == null || bootstrapServers.isEmpty()) {
            logger.error("KAFKA_BOOTSTRAP_SERVERS environment variable is not set");
            System.exit(1);
        }
        
        if (username == null || username.isEmpty()) {
            logger.error("KAFKA_USERNAME environment variable is not set");
            System.exit(1);
        }
        
        if (password == null || password.isEmpty()) {
            logger.error("KAFKA_PASSWORD environment variable is not set");
            System.exit(1);
        }
        
        if (topic == null || topic.isEmpty()) {
            logger.error("KAFKA_TOPIC environment variable is not set");
            System.exit(1);
        }
        
        logger.info("Starting Kafka Consumer...");
        logger.info("Bootstrap Servers: {}", bootstrapServers);
        logger.info("Topic: {}", topic);
        logger.info("Group ID: {}", groupId);
        logger.info("Security Protocol: {}", securityProtocol);
        
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        
        props.put("security.protocol", securityProtocol);
        props.put("sasl.mechanism", "SCRAM-SHA-512");
        
        props.put("ssl.endpoint.identification.algorithm", "");
        
        if ("SASL_SSL".equals(securityProtocol)) {
            try {
                String truststorePath = extractCertFromResources("certs/truststore.jks");
                String keystorePath = extractCertFromResources("certs/keystore.p12");
                String truststorePassword = System.getenv().getOrDefault("KAFKA_TRUSTSTORE_PASSWORD", "password");
                String keystorePassword = System.getenv().getOrDefault("KAFKA_KEYSTORE_PASSWORD", "password");
                
                props.put("ssl.truststore.location", truststorePath);
                props.put("ssl.truststore.password", truststorePassword);
                props.put("ssl.truststore.type", "JKS");
                props.put("ssl.keystore.location", keystorePath);
                props.put("ssl.keystore.password", keystorePassword);
                props.put("ssl.keystore.type", "PKCS12");
                props.put("ssl.key.password", keystorePassword);
                logger.info("Using SSL/TLS with truststore: {} and keystore: {}", truststorePath, keystorePath);
            } catch (Exception e) {
                logger.error("Failed to load certificates from resources: {}", e.getMessage(), e);
                System.exit(1);
            }
        }
        
        String jaasConfig = "org.apache.kafka.common.security.scram.ScramLoginModule required " +
                           "username=\"" + username + "\" " +
                           "password=\"" + password + "\";";
        props.put("sasl.jaas.config", jaasConfig);
        
        KafkaConsumer<String, String> consumer = null;
        
        try {
            consumer = new KafkaConsumer<>(props);
            consumer.subscribe(Collections.singletonList(topic));
            
            logger.info("Successfully subscribed to topic: {}", topic);
            logger.info("Polling for messages...");
            
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
                
                if (!records.isEmpty()) {
                    logger.info("Received {} messages", records.count());
                }
                
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("========================================");
                    logger.info("Topic: {}", record.topic());
                    logger.info("Partition: {}", record.partition());
                    logger.info("Offset: {}", record.offset());
                    logger.info("Key: {}", record.key());
                    logger.info("Value: {}", record.value());
                    logger.info("Timestamp: {}", record.timestamp());
                    logger.info("========================================");
                }
            }
            
        } catch (Exception e) {
            logger.error("Error in Kafka Consumer: {}", e.getMessage(), e);
        } finally {
            if (consumer != null) {
                logger.info("Closing Kafka Consumer...");
                consumer.close();
            }
        }
    }
}
