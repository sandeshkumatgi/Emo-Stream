from kafka import KafkaProducer, KafkaConsumer
import json

# Main Publisher setup to read from Spark's output topic
main_publisher_consumer = KafkaConsumer(
    'output_topic',  # This is where the Spark Streaming job writes
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Setup for publishing to cluster topics
cluster_producer= KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Main publisher loop to read from output_topic and forward to clusters
print("Main Publisher: Listening for messages on 'output_topic'")
#for message in main_publisher_consumer:
 #   key = message.key.decode('utf-8') if message.key else None  # Decode key if present
  #  value = message.value
   # print(f"Key: {key}, Value: {value}")
for message in main_publisher_consumer:
    cluster_producer.send('c1', value=(message.key.decode('utf-8'),message.value))
    cluster_producer.send('c2', value=(message.key.decode('utf-8'),message.value))
    cluster_producer.send('c3', value=(message.key.decode('utf-8'),message.value))

