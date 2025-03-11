from kafka import KafkaProducer
import time

# Kafka configuration
bootstrap_servers = 'localhost:9092'
topic_name = 'test-topic'

# Create Kafka producer
producer = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: str(v).encode('utf-8')  # Serialize messages to bytes
)

# Send 1 message to Kafka topic
#for i in range(100000):
#    message = f"Message {i}"
#    print(f"Sending: {message}")
#    producer.send(topic_name, value=message)
#    time.sleep(5)  # Wait for 1 second between messages


# Send multiple messages in a single batch
messages = [f"Message {i}" for i in range(1000000)]  # Create a list of 10 messages
for message in messages:
    print(f"Sending: {message}")
    producer.send(topic_name, value=message)


# Flush and close the producer
producer.flush()
producer.close()