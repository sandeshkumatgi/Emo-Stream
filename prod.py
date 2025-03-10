from kafka import KafkaProducer
from json import dumps
import random
import time
import datetime

# Initialize Kafka Producer with a flush interval of 500ms
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],  # Kafka broker address
    value_serializer=lambda v: dumps(v).encode('utf-8'),  # Serialize data to JSON
    linger_ms=500  # Time to buffer messages before sending them (500ms)
)

# List of sample emojis
#emoji_list = ["😊", "😂", "❤️", "👍", "😢", "🔥", "🎉", "🙌", "🎈", "💯", "😎", "🥳", "🤔", "😮", "😭", "😡"]
emoji_list = ["😊", "😂", "😢", "❤️", "🔥"]
emoji_weights = [0.4, 0.3, 0.2, 0.05, 0.05] 
# Function to generate random emoji data
def generate_emoji_data():
    user_id = random.randint(1000, 9999)  # Random user ID
    #emoji_type = random.choice(emoji_list)  # Random emoji
    emoji_type = random.choices(emoji_list, weights=emoji_weights, k=1)[0]
    timestamp = datetime.datetime.utcnow().isoformat()  # Current UTC timestamp
    return {
        "user_id": str(user_id),
        "emoji_type": emoji_type,
        "timestamp": timestamp
    }

# Continuous loop to send emoji data to Kafka
try:
    print("Starting to send emoji data to Kafka with a 500ms flush interval...")
    while True:
        messages = []  # Batch of messages
        
        # Generate hundreds of messages
        for _ in range(1000):
            emoji_data = generate_emoji_data()
            messages.append(emoji_data)
            producer.send('emoji_topic', value=emoji_data)
        
        # Print batch size and a sample message
        print(f"Sent batch of {len(messages)} messages. Sample: {messages[0]}")
        
        # Wait for 500 milliseconds before starting the next batch
        time.sleep(0.5)
except KeyboardInterrupt:
    print("Stopping emoji data generation...")
finally:
    producer.close()
