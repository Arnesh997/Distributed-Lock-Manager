import threading
import time
import logging
from grpc_client import LockClient  # Import your LockClient implementation
import sys

class FlushStreamHandler(logging.StreamHandler):
    def emit(self, record):
        super().emit(record)
        self.flush()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', handlers=[FlushStreamHandler(sys.stdout)])

# List of server addresses
SERVER_ADDRESSES = ["localhost:50051", "localhost:50052", "localhost:50053"]

def test_primary_node_failure_and_recovery():
    # Define client tasks
    def client1_task(client, client_id, character):
        logging.info(f"Client {client_id} starting operations.")
        client.client_init(client_id)
        for _ in range(5):  # Append the character 5 times
            client.lock_acquire(client_id)
            logging.info(f"Client {client_id} acquired the lock.")
            client.file_append(client_id, "file_1", character)
            logging.info(f"Client {client_id} appended '{character}' to file_1.")
            client.lock_release(client_id)
            logging.info(f"Client {client_id} released the lock.")
        client.client_close(client_id)
        logging.info(f"Client {client_id} completed operations.")

    def client_task(client, client_id, character):
        logging.info(f"Client {client_id} starting operations.")
        client.client_init(client_id)
        for _ in range(5):  # Append the character 5 times
            client.lock_acquire(client_id)
            logging.info(f"Client {client_id} acquired the lock.")
            client.file_append(client_id, "file_1", character)
            logging.info(f"Client {client_id} appended '{character}' to file_1.")
            client.lock_release(client_id)
            logging.info(f"Client {client_id} released the lock.")
        client.client_close(client_id)
        logging.info(f"Client {client_id} completed operations.")

    # Initialize clients with unique IDs and tasks
    client1 = LockClient(addresses=SERVER_ADDRESSES)
    client2 = LockClient(addresses=SERVER_ADDRESSES)
    client3 = LockClient(addresses=SERVER_ADDRESSES)

    # Start Client 1 in its own thread
    client1_thread = threading.Thread(target=client1_task, args=(client1, 201, "A"))
    client1_thread.start()

    # Wait for Client 1 to complete
    client1_thread.join()
    logging.info("Client 1 completed. You have 10 seconds to crash the primary server (Server 1).")
    time.sleep(15)  # Allow manual server crash

    # Start Client 2 and Client 3 in their own threads
    client2_thread = threading.Thread(target=client_task, args=(client2, 202, "B"))
    client3_thread = threading.Thread(target=client_task, args=(client3, 203, "C"))

    client2_thread.start()
    time.sleep(15)  # Allow Client 2 to acquire and release the lock
    client3_thread.start()


    
    # Wait for all threads to complete
    client2_thread.join()
    client3_thread.join()

    logging.info("Now restart the dead server. Verify file contents for consistency.")

if __name__ == "__main__":
    test_primary_node_failure_and_recovery()