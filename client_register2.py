import socket
import threading
import requests
import json

HOST = '127.0.0.1'  # The server's hostname or IP address
PORT = 65430        # The port used by the server
APP_URL = 'http://127.0.0.1:5000/emoji'  # URL for app.py's /emoji endpoint

def receive_from_server(client_socket):
    """Function to receive data from load.py (server)."""
    try:
        while True:
            # Receive data from the server
            data = client_socket.recv(1024)
            if not data:
                print("Server has closed the connection.")
                break
            message = data.decode('utf-8')
            print(f"Received from server: {message}")  # Log the received message
    except Exception as e:
        print(f"Error receiving data: {e}")

def send_to_app(data):
    """Function to send user input to app.py."""
    try:
        # Create a JSON payload with the data
        payload = {
            "emoji_type": data,
            "user_id": "1234"  # Replace with actual client identifier if needed
        }
        # Send a POST request to app.py
        response = requests.post(APP_URL, json=payload)
        print(f"Sent to app.py: {payload}, Response: {response.status_code} {response.text}")
    except Exception as e:
        print(f"Error sending data to app.py: {e}")

def handle_user_input(client_socket):
    """Function to take user input and send it to app.py."""
    try:
        while True:
            user_input = input("Enter a message to send to app.py (type 'exit' to quit): ").strip()
            if user_input.lower() == 'exit':
                print("Exiting...")
                client_socket.close()  # Close the socket to stop receiving data
                break
            if user_input:
                # Send user input to app.py
                threading.Thread(target=send_to_app, args=(user_input,)).start()
    except Exception as e:
        print(f"Error handling user input: {e}")

def main():
    """Main function to handle connection and threads."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as client_socket:
        # Connect to the server (load.py)
        client_socket.connect((HOST, PORT))
        print(f"Connected to server at {HOST}:{PORT}")

        try:
            # Start a thread to handle receiving data
            receiver_thread = threading.Thread(target=receive_from_server, args=(client_socket,))
            receiver_thread.daemon = True  # Ensure this thread closes when the main thread exits
            receiver_thread.start()

            # Handle user input in the main thread
            handle_user_input(client_socket)

        except KeyboardInterrupt:
            print("Exiting Gracefully")
        except Exception as e:
            print(f"Error in main: {e}")

if __name__ == "__main__":
    main()

