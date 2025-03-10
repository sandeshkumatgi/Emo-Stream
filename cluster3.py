from kafka import KafkaConsumer, KafkaProducer
import json

# Cluster Publisher setup for "Cluster 1"
cluster_publisher_consumer = KafkaConsumer(
    'c3',
    bootstrap_servers=['localhost:9092'],
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Subscribers will listen to this topic to get the broadcasted messages
subscriber_producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)
for message in cluster_publisher_consumer:
    #print(message.value[0]*message.value[1])
# Cluster publisher loop to receive messages and send to subscribers
#for message in cluster_publisher_consumer:
    data=message.value[0]*message.value[1]
    print(data)
    subscriber_producer.send('s7', value=data)
    subscriber_producer.send('s8', value=data)
    subscriber_producer.send('s9', value=data)
