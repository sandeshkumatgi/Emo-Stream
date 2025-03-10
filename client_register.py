import socket

HOST = '127.0.0.1'  # The server's hostname or IP address
PORT = 65430        # The port used by the server

# Create a client socket and connect to the server
with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((HOST, PORT))  # Connect to the server
    print(f"Connected to server at {HOST}:{PORT}")
    
    try:
        while True:
            # Receive data from the server (Kafka messages)
            data = s.recv(1024)  # You can adjust the buffer size as needed
            if not data:
                # No data received (server closed the connection)
                print("Server has closed the connection.")
                break
            # Print the received message
            print(f"Received from server: {data.decode('utf-8')}")
    except KeyboardInterrupt:
        print("Client interrupted.")

