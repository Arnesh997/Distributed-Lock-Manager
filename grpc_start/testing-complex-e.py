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

def test_multiple_server_node_failures():
    def client_task(client, client_id, character, files, hold_time=1.5, notify_crash=None):
        client.client_init(client_id)
        for file in files:
            for _ in range(2):  # Append twice per file
                client.lock_acquire(client_id)
                logging.info(f"Client {client_id} acquired lock for {file}.")
                client.file_append(client_id, file, character)
                logging.info(f"Client {client_id} appended '{character}' to {file}.")
                time.sleep(hold_time)  # Simulate lock holding
                client.lock_release(client_id)
                logging.info(f"Client {client_id} released lock for {file}.")

                # Trigger the first crash during Client 1's operations
                if notify_crash and notify_crash.is_set():
                    logging.info("Simulating crash of Server 2...")
                    notify_crash.clear()  # Reset the event after the crash

        client.client_close(client_id)
        logging.info(f"Client {client_id} completed operations.")

    # Initialize clients
    client1 = LockClient(addresses=SERVER_ADDRESSES)
    client2 = LockClient(addresses=SERVER_ADDRESSES)
    client3 = LockClient(addresses=SERVER_ADDRESSES)

    # Files to append to
    files = [f"file_{i}" for i in range(1, 6)]

    # Start Client 1 operations
    logging.info("Starting Client 1...")
    client1_thread = threading.Thread(target=client_task, args=(client1, 201, "A", files))
    client1_thread.start()
    client1_thread.join()
    
    # Start Client 2 operations
    logging.info("Starting Client 2...")
    client2_thread = threading.Thread(target=client_task, args=(client2, 202, "B", files))
    client2_thread.start()
    client2_thread.join()

    # Start Client 3 operations
    logging.info("Starting Client 3...")
    client3_thread = threading.Thread(target=client_task, args=(client3, 203, "C", files))
    client3_thread.start()
    client3_thread.join()

    logging.info("All client operations completed. Verify file contents for consistency.")

if __name__ == "__main__":
    test_multiple_server_node_failures()