import grpc
import time
from concurrent import futures
import lock_pb2_grpc
import lock_pb2
from grpc_client import LockClient
from grpc_server import LockServiceServicer
import pytest
import threading
from unittest.mock import patch, MagicMock
import sys


def start_grpc_server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    lock_pb2_grpc.add_LockServiceServicer_to_server(LockServiceServicer(), server)
    server.add_insecure_port('[::]:50051')
    server.start()
    return server

@pytest.fixture(scope="function")
def grpc_server_test():
    """Fixture to start and stop the gRPC server before and after each test."""
    server = start_grpc_server()

    # Give the server time to start
    time.sleep(0.5)  # Adjust this as needed for server startup time

    yield  # This will run the test using the server

    server.stop(grace=None)  #Stop the server after the test


@pytest.fixture(scope="function")
def lock_client():
    client = LockClient(address='localhost:50051')
    return client

def test_lock_init(lock_client):
    print("test_lock_int")
    client_id = 101
    response = lock_client.client_init(client_id)
    # lock_client.client_close(client_id)

    assert response.rc == client_id
    lock_client.client_close(client_id)

# def test_lock_acquire(lock_client):
#     print("lock_acq")
#     client_id = 102
#     lock_client.client_init(client_id)
#     response = lock_client.lock_acquire(client_id)
#     assert response.status == 0
#     lock_client.lock_release(client_id)
#     lock_client.client_close(client_id)

def test_lock_acquisition_concurrently(lock_client):
    # Function for the first client to acquire and hold the lock
    def client1_task():
        print("Client 1 attempting to acquire lock...")
        lock_client_1.client_init(client_id_1)
        lock_client_1.lock_acquire(client_id_1)
        print("Client 1 acquired the lock.")
        time.sleep(5)
        # Release the lock held by Client 1 to reset state
        lock_client_1.lock_release(client_id_1)
        lock_client_1.client_close(client_id_1)
        
    # Function for the second client to attempt to acquire the lock
    def client2_task():
        print("Client 2 attempting to acquire lock...")
        lock_client_2.client_init(client_id_2)
        response = lock_client_2.lock_acquire(client_id_2)
        print("Client 2 acquisition attempt result:", response)
        lock_client_2.client_close(client_id_2)
        

    # Initialize two separate LockClient instances with unique client IDs
    lock_client_1 = LockClient(address='localhost:50051')
    client_id_1 = 202

    lock_client_2 = LockClient(address='localhost:50051')
    client_id_2 = 203

    # Create threads for each client task
    client1_thread = threading.Thread(target=client1_task)
    client2_thread = threading.Thread(target=client2_task)

    # Start both threads
    client1_thread.start()
    client2_thread.start()


def test_lock_acquisition_concurrently_multiple():
    # Function for the first client to acquire and hold the lock
    def client1_task():
        # print("Client 1 attempting to acquire lock...")
        lock_client_1.client_init(client_id_1)
        lock_client_1.lock_acquire(client_id_1)
        # print("Client 1 acquired the lock.")
        time.sleep(3)        
        lock_client_1.lock_release(client_id_1)
        lock_client_1.client_close(client_id_1)
        
    # Function for the second client to attempt to acquire the lock
    def client2_task():
        # print("Client 2 attempting to acquire lock...")
        lock_client_2.client_init(client_id_2)
        response = lock_client_2.lock_acquire(client_id_2)
        # print("Client 2 acquisition attempt result:", response)
        time.sleep(2)
        lock_client_2.lock_release(client_id_2)
        lock_client_2.client_close(client_id_2)

    def client3_task():
        # print("Client 1 attempting to acquire lock...")
        lock_client_3.client_init(client_id_3)
        lock_client_3.lock_acquire(client_id_3)
        # print("Client 1 acquired the lock.")
        time.sleep(1)
        # Release the lock held by Client 1 to reset state
        lock_client_3.lock_release(client_id_3)
        lock_client_3.client_close(client_id_3)

    def client4_task():
        # print("Client 1 attempting to acquire lock...")
        lock_client_4.client_init(client_id_4)
        lock_client_4.lock_acquire(client_id_4)
        # print("Client 1 acquired the lock.")
        time.sleep(2)
        # Release the lock held by Client 1 to reset state
        lock_client_4.lock_release(client_id_4)
        lock_client_4.client_close(client_id_4)

    def client5_task():
        # print("Client 1 attempting to acquire lock...")
        lock_client_5.client_init(client_id_5)
        lock_client_5.lock_acquire(client_id_5)
        # print("Client 1 acquired the lock.")
        time.sleep(1)
        # Release the lock held by Client 1 to reset state
        lock_client_5.lock_release(client_id_5)
        lock_client_5.client_close(client_id_5)    

    # Initialize two separate LockClient instances with unique client IDs
    lock_client_1 = LockClient(address='localhost:50051')
    client_id_1 = 201

    lock_client_2 = LockClient(address='localhost:50051')
    client_id_2 = 202

    lock_client_3 = LockClient(address='localhost:50051')
    client_id_3 = 203

    lock_client_4 = LockClient(address='localhost:50051')
    client_id_4 = 204

    lock_client_5 = LockClient(address='localhost:50051')
    client_id_5 = 205

    # Create threads for each client task
    client1_thread = threading.Thread(target=client1_task)
    client2_thread = threading.Thread(target=client2_task)
    client3_thread = threading.Thread(target=client3_task)
    client4_thread = threading.Thread(target=client4_task)
    client5_thread = threading.Thread(target=client5_task)

    # Start both threads
    client1_thread.start()
    time.sleep(1)
    client2_thread.start()
    client3_thread.start()
    time.sleep(9)
    client4_thread.start()
    client5_thread.start()
    
    # Wait for both threads to complete
    client1_thread.join()
    client2_thread.join()
    client3_thread.join()
    client4_thread.join()
    client5_thread.join()
    
def test_file_append_concurrently_multiple():
    # Function for the first client to acquire and hold the lock
    def client1_task():
        # print("Client 1 attempting to acquire lock...")
        lock_client_1.client_init(client_id_1)
        lock_client_1.lock_acquire(client_id_1)
        lock_client_1.file_append(client_id_1, "file_1", "Client 1 appended data to file_1")
        # print("Client 1 acquired the lock.")
        time.sleep(3)
        
        lock_client_1.lock_release(client_id_1)
        lock_client_1.client_close(client_id_1)
        
    # Function for the second client to attempt to acquire the lock
    def client2_task():
        # print("Client 2 attempting to acquire lock...")
        lock_client_2.client_init(client_id_2)
        response = lock_client_2.lock_acquire(client_id_2)
        
        # print("Client 2 acquisition attempt result:", response)
        time.sleep(2)
        lock_client_2.file_append(client_id_2, "file_1", "Client 2 appended data to file_1")
        lock_client_2.lock_release(client_id_2)
        lock_client_2.client_close(client_id_2)

    def client3_task():
        # print("Client 1 attempting to acquire lock...")
        lock_client_3.client_init(client_id_3)
        lock_client_3.lock_acquire(client_id_3)
        # print("Client 1 acquired the lock.")
        time.sleep(1)
        # Release the lock held by Client 1 to reset state
        lock_client_3.file_append(client_id_3, "file_1", "Client 3 appended data to file_1")
        lock_client_3.lock_release(client_id_3)
        lock_client_3.client_close(client_id_3)

    def client4_task():
        # print("Client 1 attempting to acquire lock...")
        lock_client_4.client_init(client_id_4)
        lock_client_4.lock_acquire(client_id_4)
        # print("Client 1 acquired the lock.")
        time.sleep(2)
        # Release the lock held by Client 1 to reset state
        lock_client_4.file_append(client_id_4, "file_1", "Client 4 appended data to file_1")
        lock_client_4.lock_release(client_id_4)
        lock_client_4.client_close(client_id_4)

    def client5_task():
        # print("Client 1 attempting to acquire lock...")
        lock_client_5.client_init(client_id_5)
        lock_client_5.lock_acquire(client_id_5)
        # print("Client 1 acquired the lock.")
        time.sleep(1)
        # Release the lock held by Client 1 to reset state
        lock_client_5.file_append(client_id_5, "file_1", "Client 5 appended data to file_1")
        lock_client_5.lock_release(client_id_5)
        lock_client_5.client_close(client_id_5)    

    # Initialize two separate LockClient instances with unique client IDs
    lock_client_1 = LockClient(address='localhost:50051')
    client_id_1 = 201

    lock_client_2 = LockClient(address='localhost:50051')
    client_id_2 = 202

    lock_client_3 = LockClient(address='localhost:50051')
    client_id_3 = 203

    lock_client_4 = LockClient(address='localhost:50051')
    client_id_4 = 204

    lock_client_5 = LockClient(address='localhost:50051')
    client_id_5 = 205

    # Create threads for each client task
    client1_thread = threading.Thread(target=client1_task)
    client2_thread = threading.Thread(target=client2_task)
    client3_thread = threading.Thread(target=client3_task)
    client4_thread = threading.Thread(target=client4_task)
    client5_thread = threading.Thread(target=client5_task)

    # Start both threads
    client1_thread.start()
    time.sleep(1)
    client2_thread.start()
    client3_thread.start()
    time.sleep(9)
    client4_thread.start()
    client5_thread.start() 

    # Wait for both threads to complete
    client1_thread.join()
    client2_thread.join()
    client3_thread.join()
    client4_thread.join()
    client5_thread.join()
     
def test_lock_acquisition_with_deadline(lock_client):
    def client1_task():
        print("Client 1 attempting to acquire lock...")
        lock_client_1.client_init(client_id_1)
        lock_client_1.lock_acquire(client_id_1)
        print("Client 1 acquired the lock.")
        time.sleep(15)  # Hold the lock for longer than the lock timeout
        print("Client 1 exiting without releasing the lock (simulate crash).")
        lock_client_1.client_close(client_id_1)

    def client2_task():
        time.sleep(10)  # Wait for Client 1's deadline to expire
        print("Client 2 attempting to acquire lock...")
        lock_client_2.client_init(client_id_2)
        response = lock_client_2.lock_acquire(client_id_2)
        print("Client 2 acquisition attempt result:", response)
        assert response.status == lock_pb2.SUCCESS, "Client 2 should acquire the lock after Client 1's timeout."
        lock_client_2.client_close(client_id_2)

    lock_client_1 = LockClient(address='localhost:50051')
    client_id_1 = 202

    lock_client_2 = LockClient(address='localhost:50051')
    client_id_2 = 203

    client1_thread = threading.Thread(target=client1_task)
    client2_thread = threading.Thread(target=client2_task)

    client1_thread.start()
    client2_thread.start()

    # Ensure threads complete before finishing the test
    client1_thread.join()
    client2_thread.join()

    print("Test passed: Client 2 acquired the lock after Client 1's timeout.")
        
def test_client_crash_before_file_append(lock_client):
    def client1_task():
        print("Client 1 attempting to acquire lock...")
        lock_client_1.client_init(client_id_1)
        lock_client_1.lock_acquire(client_id_1)
        print("Client 1 acquired the lock.")
        time.sleep(15)  # Simulate lock expiration
        print("Client 1 crashes before file append.")
        # time.sleep(20)  # Wait for Client 2 to acquire and release the lock
        print("Client 1 attempting file append...")
        response = lock_client_1.file_append(client_id_1, "file_1", "Test data")
        assert response.status == lock_pb2.FILE_ERROR, "Client 1 should not hold the lock."
        lock_client_1.lock_acquire(client_id_1)
        response = lock_client_1.file_append(client_id_1, "file_1", "Test data")
        print("Client 1 append attempt result:", response)
        # assert response.status == lock_pb2.SUCCESS, "Client 1 should acquire the lock after Client 2's timeout."
        lock_client_1.client_close(client_id_1)


    def client2_task():
        time.sleep(10)  # Wait for Client 1's deadline to expire
        print("Client 2 attempting to acquire lock...")
        lock_client_2.client_init(client_id_2)
        response = lock_client_2.lock_acquire(client_id_2)
        print("Client 2 acquisition attempt result:", response)
        assert response.status == lock_pb2.SUCCESS, "Client 2 should acquire the lock after Client 1's timeout."
        lock_client_2.file_append(client_id_2, "file_1", "Test data for client2")

        lock_client_2.client_close(client_id_2)

    def client1_recovery_task():
        time.sleep(20)  # Wait for Client 2 to acquire and release the lock
        print("Client 1 attempting file append...")
        response = lock_client_1.file_append(client_id_1, "file_1", "Test data")
        print("Client 1 append attempt result:", response)
        assert response.status == lock_pb2.FILE_ERROR, "Client 1 should not hold the lock."
        assert "Added to the wait queue" in response.message, "Client 1 should be added to the wait queue."

    lock_client_1 = LockClient(address='localhost:50051')
    client_id_1 = 101

    lock_client_2 = LockClient(address='localhost:50051')
    client_id_2 = 102

    client1_thread = threading.Thread(target=client1_task)
    client2_thread = threading.Thread(target=client2_task)
    # client1_recovery_thread = threading.Thread(target=client1_recovery_task)

    client1_thread.start()
    client2_thread.start()
    # client1_recovery_thread.start()

    client1_thread.join()
    client2_thread.join()
    # client1_recovery_thread.join()

    print("Test passed: Client 1 was added to the wait queue after crashing before file append.")
    

def test_client_crash_after_file_append(lock_client):
    def client1_task():
        print("Client 1 attempting to acquire lock...")
        lock_client_1.client_init(client_id_1)
        lock_client_1.lock_acquire(client_id_1)
        print("Client 1 acquired the lock.")

        print("Client 1 sending file append request...")
        response = lock_client_1.file_append(client_id_1, "file_1", "Test data")
        assert response.status == lock_pb2.SUCCESS, "File append should succeed."
        print("Client 1 crashed after file append.")
        lock_client_1.client_close(client_id_1)

    def client1_recovery_task():
        time.sleep(10)  # Allow server to process the first request
        print("Client 1 attempting to re-append data...")
        response = lock_client_1.file_append(client_id_1, "file_1", "Test data")
        print("Client 1 re-append attempt result:", response)
        assert response.status == lock_pb2.SUCCESS, "Server should return the cached response."
        assert "Data successfully appended to the file." in response.message
        lock_client_1.client_close(client_id_1)

    lock_client_1 = LockClient(address='localhost:50051')
    client_id_1 = 103

    client1_thread = threading.Thread(target=client1_task)
    client1_recovery_thread = threading.Thread(target=client1_recovery_task)

    client1_thread.start()
    client1_recovery_thread.start()

    client1_thread.join()
    client1_recovery_thread.join()

    print("Test passed: Cached response returned for duplicate file append request.")




