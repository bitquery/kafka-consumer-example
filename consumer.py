# This code displays latest transactions on Solana
# It can be reused for all topics, all chains by simply changing the topic, username, password and the Proto file import.

import uuid
import base58
from confluent_kafka import Consumer, KafkaError, KafkaException
from google.protobuf.message import DecodeError
from google.protobuf.descriptor import FieldDescriptor
from solana import parsed_idl_block_message_pb2
import logging
import config
import datetime
import threading
import signal

# Kafka consumer configuration
group_id_suffix = uuid.uuid4().hex
conf = {
    'bootstrap.servers': 'rpk0.bitquery.io:9092,rpk1.bitquery.io:9092,rpk2.bitquery.io:9092',
    'group.id': f'{config.username}-group-{group_id_suffix}',  
    'session.timeout.ms': 30000,
    'security.protocol': 'SASL_PLAINTEXT',
    'ssl.endpoint.identification.algorithm': 'none',
    'sasl.mechanisms': 'SCRAM-SHA-512',
    'sasl.username': config.username,
    'sasl.password': config.password,
    'auto.offset.reset': 'latest',
}

consumer = Consumer(conf)
topic = 'solana.transactions.proto' 
consumer.subscribe([topic])

# Control flag for graceful shutdown
shutdown_event = threading.Event()
processed_count = 0


# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# ---  recursive traversal and print --- #

def convert_bytes(value, encoding='base58'):
    if encoding == 'base58':
        return base58.b58encode(value).decode()
    return value.hex()

def print_protobuf_message(msg, indent=0, encoding='base58'):
    prefix = ' ' * indent
    for field in msg.DESCRIPTOR.fields:
        value = getattr(msg, field.name)

        if field.label == FieldDescriptor.LABEL_REPEATED: # The field is a repeated (i.e. array/list) field.
            if not value:
                continue
            print(f"{prefix}{field.name} (repeated):")
            for idx, item in enumerate(value):
                if field.type == FieldDescriptor.TYPE_MESSAGE: # The field is a nested protobuf message.
                    print(f"{prefix}  [{idx}]:")
                    print_protobuf_message(item, indent + 4, encoding)
                elif field.type == FieldDescriptor.TYPE_BYTES:
                    print(f"{prefix}  [{idx}]: {convert_bytes(item, encoding)}")
                else:
                    print(f"{prefix}  [{idx}]: {item}")

        elif field.type == FieldDescriptor.TYPE_MESSAGE:
            if msg.HasField(field.name):
                print(f"{prefix}{field.name}:")
                print_protobuf_message(value, indent + 4, encoding)

        elif field.type == FieldDescriptor.TYPE_BYTES:
            print(f"{prefix}{field.name}: {convert_bytes(value, encoding)}")

        elif field.containing_oneof:
            if msg.WhichOneof(field.containing_oneof.name) == field.name:
                print(f"{prefix}{field.name} (oneof): {value}")

        else:
            print(f"{prefix}{field.name}: {value}")

def process_message(buffer):
    """Process a single protobuf message"""
    try:
        tx_block = parsed_idl_block_message_pb2.ParsedIdlBlockMessage()
        tx_block.ParseFromString(buffer)

        timestamp = datetime.datetime.now(datetime.timezone.utc)


        print(f"\n Block: {tx_block.Header.Slot} | Time: {timestamp}")

        # below code will print tx signature and block number, uncommment if you need to test
        #    if hasattr(tx_block, 'Transactions') and tx_block.Transactions:
        #        tx_signature = tx_block.Transactions[0].Signature
  
        #        signature_str = base58.b58encode(tx_signature).decode()
        #        print(f"\n Transaction: {signature_str} | Block: {tx_block.Header.Slot} | Time: {timestamp}")
        #    else:
        #        print(f"\n Block: {tx_block.Header.Slot} | Time: {timestamp}")
                
            # print_protobuf_message(tx_block, encoding='base58') # uncomment this to print the message

    except DecodeError as err:
        logger.error(f"Protobuf decoding error: {err}")
    except Exception as err:
        logger.error(f"Error processing message: {err}")

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully"""
    logger.info(f"Received signal {signum}, initiating shutdown...")
    shutdown_event.set()

# --- Main execution --- #

def main():
    global processed_count
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Main thread: Kafka polling loop
    try:
        while not shutdown_event.is_set():
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    raise KafkaException(msg.error())
            try:
                process_message(msg.value())
                processed_count += 1
            except Exception as err:
                logger.exception(f"Failed to process message: {err}")
               
                
    except KeyboardInterrupt:
        logger.info("Keyboard interrupt received")
    except Exception as e:
        logger.exception(f"Error in main polling loop: {e}")
    finally:
        # Graceful shutdown
        logger.info("Initiating graceful shutdown...")
        shutdown_event.set()

        # Close Kafka consumer
        consumer.close()
        logger.info(f"Shutdown complete. Total messages processed: {processed_count}")

if __name__ == "__main__":
    main()
