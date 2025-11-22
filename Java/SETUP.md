# Setup

# Project Structure
```
kafka-consumer/
├── src/main/java/com/kafkaconsumer/
│   └── KafkaConsumerApp.java       # Main Kafka consumer implementation
├── pom.xml                          # Maven build configuration
└── .gitignore                       # Java-specific ignore patterns
```

## Configuration

### Required Environment Variables (Secrets)
The following sensitive credentials must be configured in Replit Secrets:
- `KAFKA_BOOTSTRAP_SERVERS` - Kafka broker addresses (e.g., "broker1:9092,broker2:9092")
- `KAFKA_USERNAME` - SASL authentication username
- `KAFKA_PASSWORD` - SASL authentication password
- `KAFKA_TOPIC` - Topic name to consume messages from
- `KAFKA_GROUP_ID` (optional) - Consumer group ID (default: "kafka-consumer-group")

## Setting up Certificates

1. Create PKCS12 keystore from client certificate and key:
```
openssl pkcs12 -export -in certs/client.cer.pem -inkey certs/client.key.pem -out certs/keystore.p12 -name kafka-client -password pass:password
```
2. Create JKS truststore from server certificate:
```
keytool -import -alias kafka-server -file certs/server.cer.pem -keystore certs/truststore.jks -storepass password -noprompt
```

Notes:

- Replace password with your desired passwords if needed
- The first command creates keystore.p12 (PKCS12 format) for client authentication
- The second command creates truststore.jks (JKS format) for server certificate validation
- Both files are already embedded in your application in the src/main/resources/certs/ directory
- The default passwords in the code are set to "password" - adjust the env vars if you use different passwords
