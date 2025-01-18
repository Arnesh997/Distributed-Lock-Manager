import threading
import time
import logging
import sys
from grpc_client import LockClient  # Import your LockClient implementation

class FlushStreamHandler(logging.StreamHandler):
    def emit(self, record):
        super().emit(record)
        self.flush()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s',  handlers=[FlushStreamHandler(sys.stdout)])

# List of server addresses
SERVER_ADDRESSES = ["localhost:50051", "localhost:50052", "localhost:50053"]

def test_replica_failure_slow_recovery():
    # Define client tasks
    def client_task(client, client_id, character, files, hold_time):
        """Client operation task."""
        client.client_init(client_id)
        for file in files:
            client.lock_acquire(client_id)
            logging.info(f"Client {client_id} acquired the lock.")
            client.file_append(client_id, file, character)
            logging.info(f"Client {client_id} appended '{character}' to {file}.")
            time.sleep(hold_time)  # Simulate holding the lock
            client.lock_release(client_id)
            logging.info(f"Client {client_id} released the lock.")
        client.client_close(client_id)
        logging.info(f"Client {client_id} completed operations.")

    # Initialize clients
    client1 = LockClient(addresses=SERVER_ADDRESSES)
    client2 = LockClient(addresses=SERVER_ADDRESSES)


    # Start Client 1 (append 'A' to three files)
    logging.info("Starting Client 1...")
    client1_thread = threading.Thread(
        target=client_task,
        args=(client1, 201, "A", ["file_1", "file_2", "file_3"], 0.5)
    )
    client1_thread.start()
    client1_thread.join()

    logging.info("Crash the server now")
    time.sleep(3)
    # Wait for Client 1 to start and perform its first operation
    
    # Start Client 2 (append 'B' to three files)
    logging.info("Starting Client 2...")
    client2_thread = threading.Thread(
        target=client_task,
        args=(client2, 202, "B", ["file_1", "file_2", "file_3"], 0.001)
    )
    client2_thread.start()
    client2_thread.join()
    logging.info("Start the server 2")
    time.sleep(3)

    

if __name__ == "__main__":
    test_replica_failure_slow_recovery()