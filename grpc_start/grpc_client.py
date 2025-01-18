import grpc
import lock_pb2
import lock_pb2_grpc
import logging
import time
import uuid
import argparse
from typing import Tuple, Optional
from concurrent import futures
import random

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

LOCK_ACQUIRE_MAX_TIMEOUT = 30

class LockClient:
    def __init__(self,addresses, channel=None):
        self.addresses = addresses
        self.current_primary_index = 0
        self.channel = channel
        self.stub = None

    def connect_to_any_server(self):
        # Close existing channel properly
        if self.channel:
            try:
                self.channel.close()
                time.sleep(0.5)  # Give it time to close properly
            except:
                pass
            self.channel = None
            self.stub = None
        for idx, addr in enumerate(self.addresses):
            self.current_primary_index = idx
            options = [
                ('grpc.keepalive_time_ms', 10000),
                ('grpc.keepalive_timeout_ms', 5000),
                ('grpc.keepalive_permit_without_calls', True),
                ('grpc.http2.max_pings_without_data', 0),
                ('grpc.max_reconnect_backoff_ms', 5000),
                ('grpc.min_reconnect_backoff_ms', 1000)
            ]
            self.channel = grpc.insecure_channel(addr, options=options)
            self.stub = lock_pb2_grpc.LockServiceStub(self.channel)
            logging.info(f"Trying to connect to server at {addr}")
            
            # Test connection with a small timeout
            try:
                future = grpc.channel_ready_future(self.channel)
                future.result(timeout=2)
                # Try a simple RPC call to verify connection and check if primary
                response = self.stub.client_init(lock_pb2.Int(rc=-1))
                if response and response.status == lock_pb2.Status.SUCCESS:
                    logging.info(f"Successfully connected to server at {addr}")
                    return True
                else:
                    logging.info(f"Server at {addr} is not responding properly")
            except grpc.RpcError as e:
                logging.error(f"Failed to connect to server at {addr}")
                self.channel = None
                self.stub = None
            except Exception as e:
                logging.error(f"Unexpected error connecting to server at {addr}")
                self.channel = None
                self.stub = None
                
            time.sleep(1)
        return False

    def _retry_operation(self, operation_name: str, *args, **kwargs):
        deadline = time.time() + 15  # 15 second overall deadline
        attempts = 0

        while time.time() < deadline:
            if not self.stub or not self.channel:
                if not self.connect_to_any_server():
                    continue  # Try connecting to another server
            try:
                # Fetch the operation from the stub after ensuring it's not None
                operation = getattr(self.stub, operation_name)
                result = operation(*args, **kwargs, timeout=3)

                if hasattr(result, 'status'):
                    if result.status == lock_pb2.Status.SUCCESS:
                        logging.info(f"{operation_name} successful on attempt {attempts + 1}")
                        return True, result
                    elif result.status == lock_pb2.Status.NOT_PRIMARY:
                        logging.info(f"Server {self.addresses[self.current_primary_index]} is not primary")
                        self.channel = None
                        self.stub = None
                        continue  # Try connecting to another server
                    else:
                        logging.error(f"{operation_name} failed with status: {result.status}")
                        attempts += 1
                        logging.error(f"{operation_name} failed after {attempts} attempts")
                        return False, result
                else:
                    logging.info(f"{operation_name} result has no attribute 'status'")
                    return False, result
                
            except grpc.RpcError as e:
                code = e.code() if hasattr(e, 'code') else None
                details = e.details() if hasattr(e, 'details') else str(e)
                logging.warning(f"{operation_name} failed: {code}: {details}")
                self.channel = None
                self.stub = None
                attempts += 1
                time.sleep(1)
                continue  # Try connecting to another server

            except Exception as e:
                logging.error(f"Unexpected error in {operation_name}: {str(e)}")
                self.channel = None
                self.stub = None
                attempts += 1
                continue  # Try connecting to another server

        return False, None


    def client_init(self, client_id: int) -> Optional[lock_pb2.Int]:
        success, response = self._retry_operation(
            "client_init",
            lock_pb2.Int(rc=client_id)
        )
        if success:
            logging.info(f"Client {client_id} initialized successfully")
            return response
        return None

    def lock_acquire(self, client_id: int) -> Optional[lock_pb2.Response]:
        deadline = time.time() + LOCK_ACQUIRE_MAX_TIMEOUT
        while time.time() < deadline:
            success, response = self._retry_operation(
                "lock_acquire",
                lock_pb2.lock_args(client_id=client_id)
            )
            if success:
                if response.status == lock_pb2.Status.SUCCESS:
                    logging.info(f"Client {client_id} acquired lock successfully")
                    return response
                elif response.status == lock_pb2.Status.LOCK_ACQUIRE_ERROR:
                    logging.info("Lock is held by another client, waiting...")
                    time.sleep(1)
                    continue
            time.sleep(0.5)
        logging.error(f"Failed to acquire lock after {LOCK_ACQUIRE_MAX_TIMEOUT} seconds")
        return None

    def lock_release(self, client_id: int) -> Optional[lock_pb2.Response]:
        success, response = self._retry_operation(
            "lock_release",
            lock_pb2.lock_args(client_id=client_id)
        )
        if success and response.status == lock_pb2.Status.SUCCESS:
            logging.info(f"Client {client_id} released lock successfully")
            return response
        else:
            logging.info(f"client {client_id} failed to release lock")
            return response

    def file_append(self, client_id: int, filename: str, content: str) -> Optional[lock_pb2.Response]:
        request_id = str(uuid.uuid4())
        logging.info(f"Appending to {filename} with request ID: {request_id}")

        success, response = self._retry_operation(
            "file_append",
            lock_pb2.file_args(
                filename=filename,
                content=content.encode('utf-8'),
                client_id=client_id,
                request_id=request_id
            )
        )
        if success and response.status == lock_pb2.Status.SUCCESS:
            logging.info(f"Successfully appended to {filename}")
            return response
        
        else:
            logging.error(f"Client {client_id} failed to append to file {filename}.")
            return response

    def client_close(self, client_id: int) -> Optional[lock_pb2.Response]:
        """Close client connection gracefully."""
        try:
            if self.stub:
                response = self.stub.client_close(lock_pb2.Int(rc=client_id))
                logging.info(f"Client {client_id} closed successfully")
                if self.channel:
                    self.channel.close()
                return response
        except Exception as e:
            logging.error(f"Error closing client {client_id}: {e}")
        finally:
            if self.channel:
                try:
                    self.channel.close()
                except:
                    pass
        return None

def run_client_demo(client_id: int, servers: list):
    client = LockClient(servers)

    try:
        # Initialize client
        if client.client_init(client_id) is None:
            logging.error("Failed to initialize client")
            return

        # Acquire lock
        response = client.lock_acquire(client_id)
        if response is None:
            logging.error("Failed to acquire lock")
            return

        # Append to file
        content = f"Test content from client {client_id} at {time.strftime('%Y-%m-%d %H:%M:%S')}"
        response = client.file_append(client_id, "file_1", content)
        if response is None:
            logging.error("Failed to append to file")
            client.lock_release(client_id)
            return

        # Release lock
        response = client.lock_release(client_id)
        if response is None:
            logging.error("Failed to release lock")
            return

        logging.info("All operations completed successfully")

    except KeyboardInterrupt:
        logging.info("Operation interrupted by user")
    except Exception as e:
        logging.error(f"Unexpected error: {str(e)}")
    finally:
        try:
            client.client_close(client_id)
        except Exception as e:
            logging.error(f"Error during client cleanup: {str(e)}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='gRPC Lock Client')
    parser.add_argument('--client-id', type=int, default=1, help='Client ID')
    parser.add_argument('--servers', nargs='+', default=['localhost:50051', 'localhost:50052', 'localhost:50053'],
                        help='List of server addresses (primary and replicas)')

    args = parser.parse_args()

    logging.info(f"Starting client {args.client_id} with servers: {args.servers}")
    run_client_demo(args.client_id, args.servers)
