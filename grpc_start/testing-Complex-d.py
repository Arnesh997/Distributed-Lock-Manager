import threading
import time
import logging
import sys
from grpc_client import LockClient  # Import your LockClient implementation
class FlushStreamHandler(logging.StreamHandler):
    def emit(self, record):
        super().emit(record)
        self.flush()
    
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s', handlers=[FlushStreamHandler(sys.stdout)])

# List of server addresses
SERVER_ADDRESSES = ["localhost:50051", "localhost:50052", "localhost:50053"]

def test_primary_failure_during_critical_section():
    # Define client tasks
    def client_task(client, client_id, character, repeat, hold_time, notify_crash=None):
        """Client operation task."""
        client.client_init(client_id)
        for i in range(repeat):
            client.lock_acquire(client_id)
            logging.info(f"Client {client_id} acquired the lock.")
            client.file_append(client_id, "file_40", character)
            logging.info(f"Client {client_id} appended '{character}' to file_40.")
            time.sleep(hold_time)  # Simulate holding the lock
            client.lock_release(client_id)
            logging.info(f"Client {client_id} released the lock.")
            # Notify to crash the server in the middle of Client 2's operations
            if notify_crash and i == repeat // 2:
                notify_crash.set()
        client.client_close(client_id)
        logging.info(f"Client {client_id} completed operations.")

    # Initialize clients
    client1 = LockClient(addresses=SERVER_ADDRESSES)
    client2 = LockClient(addresses=SERVER_ADDRESSES)
    client3 = LockClient(addresses=SERVER_ADDRESSES)

    # Event to notify when to crash the primary server
    crash_primary = threading.Event()

    # Start Client 1 (append 'A' 20 times)
    logging.info("Starting Client 1...")
    client1_thread = threading.Thread(target=client_task, args=(client1, 201, "A", 20, 0.5))
    client1_thread.start()
    client1_thread.join()

    # Crash primary in the middle of Client 2's operations
    logging.info("Client 1 completed. Starting Client 2 and preparing to crash the primary server...")
    
    logging.info("Crash the server once client two starts appending the character B ")
    time.sleep(5)
    client2_thread = threading.Thread(target=client_task, args=(client2, 202, "B", 20, 0.5))
    client2_thread.start()
    client2_thread.join()

    # time.sleep(25)
    # Start Client 3 (append 'C' 20 times) after failover
    logging.info("Starting Client 3...")
    client3_thread = threading.Thread(target=client_task, args=(client3, 203, "C", 20, 0.5))
    client3_thread.start()

    # Wait for all threads to complete
    
    client3_thread.join()

    logging.info("All client operations completed. Verify file contents for consistency.")

if __name__ == "__main__":
    test_primary_failure_during_critical_section()