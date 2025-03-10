import socket
import threading

# List of backend servers with counters for active connections
SERVERS = [
    {'address': ('127.0.0.1', 65432), 'connections': 0},
    {'address': ('127.0.0.1', 65431), 'connections': 0},
    {'address': ('127.0.0.1', 65433), 'connections': 0},
    {'address': ('127.0.0.1', 65434), 'connections': 0},
    {'address': ('127.0.0.1', 65435), 'connections': 0},
    {'address': ('127.0.0.1', 65436), 'connections': 0},
    {'address': ('127.0.0.1', 65437), 'connections': 0},
    {'address': ('127.0.0.1', 65438), 'connections': 0},
    {'address': ('127.0.0.1', 65439), 'connections': 0}
]

MAX_CONNECTIONS = 5  # Maximum number of connections per server
current_server = 0


def handle_client(client_socket, server_address):
    """Receive data from the backend server and send it to the client."""
    try:
        # Connect to the selected backend server
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as backend_socket:
            backend_socket.connect(server_address)

            # Forward client's initial message to the backend server
            while True:
                response = backend_socket.recv(1024)
                if not response:
                    break
                client_socket.sendall(response)

                # Continue relaying if the backend server sends more data
                while True:
                    additional_response = backend_socket.recv(1024)
                    if not additional_response:
                        break
                    client_socket.sendall(additional_response)
    except Exception as e:
        print(f"Error handling client: {e}")
    finally:
        client_socket.close()
        # Decrement the connection count for the server after the client disconnects
        update_server_connection_count(server_address, -1)


def update_server_connection_count(server_address, delta):
    """Update the server's connection count based on the given delta."""
    for server in SERVERS:
        if server['address'] == server_address:
            server['connections'] += delta
            print(f"Server {server_address} now has {server['connections']} connections.")
            break


def load_balancer():
    """Main function to handle incoming client connections."""
    global current_server
    HOST = '127.0.0.1'  # Load balancer address
    PORT = 65430        # Load balancer port

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as lb_socket:
        lb_socket.bind((HOST, PORT))
        lb_socket.listen()
        print(f"Load balancer running on {HOST}:{PORT}")

        while True:
            client_socket, client_address = lb_socket.accept()
            print(f"Incoming connection from {client_address}")

            # Find the next available server with space for a new user
            server_address = None
            for i in range(len(SERVERS)):
                server = SERVERS[(current_server + i) % len(SERVERS)]
                if server['connections'] < MAX_CONNECTIONS:
                    server_address = server['address']
                    # Increment the connection count for the selected server
                    server['connections'] += 1
                    break

            if server_address is None:
                # All servers are at capacity
                print("All servers are at capacity. Dropping connection.")
                client_socket.close()
                continue

            # Move to the next server for the next connection
            current_server = (current_server + 1) % len(SERVERS)

            # Handle the client in a new thread
            thread = threading.Thread(target=handle_client, args=(client_socket, server_address))
            thread.start()


if __name__ == "__main__":
    load_balancer()

