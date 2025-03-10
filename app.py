from flask import Flask, request, jsonify
from kafka import KafkaProducer
from json import dumps
import datetime

app = Flask(__name__)

# Initialize Kafka producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: dumps(v).encode('utf-8')
)

@app.route('/emoji', methods=['POST'])
def emoji_stream():
    try:
        # Get the JSON data from the POST request
        data = request.get_json()

        # Add timestamp to the data (similar to prod.py)
        data['timestamp'] = datetime.datetime.utcnow().isoformat()

        # Send to Kafka
        producer.send('emoji_topic', value=data)
        producer.flush()

        return jsonify({"status": "success"}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, port=5000)

