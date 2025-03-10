import socket
import threading
import json
from kafka import KafkaConsumer

# Kafka consumer setup
consumer = KafkaConsumer(
    's6',  # Kafka topic name
    bootstrap_servers='localhost:9092',  # Kafka server address
    group_id='my-group',  # Consumer group
    auto_offset_reset='earliest'  # Start reading from the earliest message
)

# Server setup
HOST = '127.0.0.1'  # Localhost
PORT = 65436        # Port to bind the server
clients = []         # List to hold connected clients

# Function to broadcast messages to all clients
def broadcast_messages():
    try:
        for message in consumer:
            data = message.value.decode('utf-8')
            parsed_data = json.loads(data)
            if isinstance(parsed_data, str):  # Ensure it's a string
                data = parsed_data
            print(f"Broadcasting message: {data}")
            # Send data to all connected clients
            for client in clients:
                try:
                    client.sendall(data.encode('utf-8'))
                except Exception as e:
                    print(f"Error sending to client: {e}")
                    clients.remove(client)
    except Exception as e:
        print(f"Error in Kafka consumer: {e}")

# Function to handle each client connection
def handle_client(client_socket):
    print(f"Client {client_socket.getpeername()} connected.")
    clients.append(client_socket)
    try:
        while True:
            # Keep the client connection alive (optional for further interactions)
            pass
    except Exception as e:
        print(f"Error with client {client_socket.getpeername()}: {e}")
    finally:
        print(f"Closing connection to client {client_socket.getpeername()}")
        clients.remove(client_socket)
        client_socket.close()

# Function to start the server
def start_server():
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((HOST, PORT))
    server_socket.listen(5)
    print(f"Server started at {HOST}:{PORT}")
    
    # Start Kafka consumer in a separate thread
    threading.Thread(target=broadcast_messages, daemon=True).start()
    
    try:
        while True:
            # Accept new client connections
            client_socket, client_address = server_socket.accept()
            print(f"New connection from {client_address}")
            client_handler = threading.Thread(target=handle_client, args=(client_socket,))
            client_handler.start()
    except KeyboardInterrupt:
        print("\nKeyboard interrupt received. Shutting down the server.")
    except Exception as e:
        print(f"Error in server: {e}")
    finally:
        server_socket.close()
        print(f"Server socket closed. Port {PORT} is released.")


# Run the server
if __name__ == '__main__':
    start_server()

