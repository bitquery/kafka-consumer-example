from confluent_kafka import Consumer, KafkaError, KafkaException
import uuid
import ssl
from pathlib import Path

# pre-requisites

username = '<YOUR USERNAME>'
password = '<YOUR PASSWORD>'
topic = '<TOPIC>' # ' e.g. tron.broadcasted.transactions

# end of pre-requisites

# Kafka consumer configuration
conf = {
    'bootstrap.servers': 'rpk0.bitquery.io:9092,rpk1.bitquery.io:9092,rpk2.bitquery.io:9092',
    'group.id': username + '-mygroup',  # Generate a unique group ID
    'session.timeout.ms': 30000,
    'security.protocol': 'SASL_PLAINTEXT',
    'ssl.endpoint.identification.algorithm': 'none',
    'sasl.mechanisms': 'SCRAM-SHA-512',
    'sasl.username': username,
    'sasl.password': password,
    'auto.offset.reset': 'latest',
    "enable.auto.commit":  'false',
}

# Initialize Kafka consumer
consumer = Consumer(conf)

# Function to process each message
def process_message(message):
    try:
        buffer = message.value()
        # Log message data
        log_entry = {
            'partition': message.partition(),
            'offset': message.offset(),
            'value': buffer.decode('utf-8')
        }
        print(log_entry)

    except Exception as err:
        print(f'Error processing message: {err}')

# Subscribe to the topic
consumer.subscribe([topic])

# Poll messages and process them
try:
    while True:
        msg = consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                raise KafkaException(msg.error())
        process_message(msg)

except KeyboardInterrupt:
    pass

finally:
    # Close down consumer
    consumer.close()
