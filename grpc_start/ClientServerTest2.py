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
from grpc_interceptor import SimulateErrorInterceptor
import subprocess
import shutil
import os

FILE_DIR = './server_files'


def start_server():
    server_process = subprocess.Popen(['python', './grpc_server.py'])  # Update this path to your grpc_server.py
    return server_process


def simulate_client_operations(client_id, filename, content):
    client = LockClient(address='localhost:50051')
    client.client_init(client_id)
    client.lock_acquire(client_id)
    client.file_append(client_id, filename, content)


def delete_server_files_directory():
    if os.path.exists(FILE_DIR):
        shutil.rmtree(FILE_DIR)
        print(f"Deleted directory: {FILE_DIR}")
    else:
        print("No directory found to delete.")


def test_server_crash_recovery():
    # Start the server
    server_process = start_server()
    time.sleep(2)  # Wait for the server to start

    client_id = 1
    filename = "file_1"
    content_before_crash = "Data before crash."

    # Simulate client operations before crash
    client_thread = threading.Thread(target=simulate_client_operations, args=(client_id, filename, content_before_crash))
    client_thread.start()
    client_thread.join()

    # Simulate server crash
    print("Simulating server crash...")
    server_process.terminate()
    server_process.wait()
    print("Server crashed.")
    
    delete_server_files_directory()  # Delete server files directory after crash
    
    time.sleep(2)  # Wait before restarting the server

    # Restart the server
    server_process = start_server()
    print("Server restarted.")
    time.sleep(2)  # Wait for the server to load state

    # Simulate client operations after restart
    content_after_crash = "Data after crash."
    client_thread = threading.Thread(target=simulate_client_operations, args=(client_id, filename, content_after_crash))
    client_thread.start()
    client_thread.join()

    # Check the file contents
    with open(f'{FILE_DIR}/{filename}.txt', 'r') as f:
        contents = f.read()
        print("File contents after recovery:")
        print(contents)

    # Clean up
    server_process.terminate()
    server_process.wait()


# Test to simulate the client retrying due to a simulated error on specific attempts
def test_retry_acquire_lock_with_simulated_packet_loss():
    # Use the static method from SimulateErrorInterceptor to create the intercepted channel
    intercept_channel = SimulateErrorInterceptor.create_intercepted_channel(method_name="lock_acquire", error_attempts=[1, 2])
    SERVER_ADDRESSES = ["localhost:50051", "localhost:50052", "localhost:50053"]
    client = LockClient(SERVER_ADDRESSES, channel=intercept_channel)

    # Specify the attempts that should simulate an error (e.g., on the 1st and 2nd attempts)
    client_id = 101

    # Client tries to acquire the lock - will fail on the specified attempt and retry
    response = client.lock_acquire(client_id)

    # Check that the client retries and eventually succeeds
    assert response is not None
    assert response.status == lock_pb2.SUCCESS


def test_retry_file_append_with_simulated_packet_loss():
    # Use the static method from SimulateErrorInterceptor to create the intercepted channel
    intercept_channel = SimulateErrorInterceptor.create_intercepted_channel(method_name="file_append", error_attempts=[1, 2])
    client = LockClient(channel=intercept_channel)

    # Specify the attempts that should simulate an error (e.g., on the 1st and 2nd attempts)
    client_id = 201

    client.lock_acquire(client_id)
    response = client.file_append(client_id, "file_1", "Client 1 appended data to file_1")


    # Check that the client retries and eventually succeeds
    assert response is not None
    assert response.status == lock_pb2.SUCCESS


   