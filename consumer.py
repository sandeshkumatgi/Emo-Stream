from kafka import KafkaConsumer
import json

# Initialize the Kafka consumer
consumer = KafkaConsumer(
    'emoji_topic',                           # Topic name
    bootstrap_servers=['localhost:9092'],     # Kafka broker address
    auto_offset_reset='earliest',             # Start reading at the earliest message
    enable_auto_commit=True,                  # Auto-commit offsets
    group_id='emoji-consumer-group',          # Consumer group ID
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))  # Deserialize JSON
)

# Process each message in the topic
print("Starting consumer... Listening for messages on 'emoji_topic'")
for message in consumer:
    emoji_data = message.value  # The message payload (emoji data)
    print(f"Received message: {emoji_data}")
