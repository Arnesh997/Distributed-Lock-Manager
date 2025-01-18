import grpc
from grpc_client import LockClient
import lock_pb2
import lock_pb2_grpc
from typing import Callable, List, Any

class SimulateErrorInterceptor(grpc.UnaryUnaryClientInterceptor):
    def __init__(self, method_name: str, error_attempts: List[int] = None):
        """
        Intercepts the specified method to modify the response on specified attempts to simulate errors.
        
        Args:
            method_name (str): The name of the method to simulate an error for (e.g., '/lock_service.LockService/lock_acquire').
            error_attempts (List[int]): List of call counts on which to simulate errors.
        """
        self.method_name = method_name
        self.call_count = 0
        self.error_attempts = error_attempts or []

    def intercept_unary_unary(self, continuation: Callable, client_call_details, request) -> Any:
        # Only intercept the specified method
        if client_call_details.method == self.method_name:
            self.call_count += 1

            # Call the actual server and get the response
            response = continuation(client_call_details, request)

            # If the current call count matches any of the specified error attempts, modify the response
            if self.call_count in self.error_attempts:
                # Modify the response to simulate an error
                raise grpc.RpcError(grpc.StatusCode.DEADLINE_EXCEEDED, "Simulated timeout error")

            # Otherwise, return the actual server response
            return response
        
        # For other methods, just proceed with the actual server call
        return continuation(client_call_details, request)

    @staticmethod
    def create_intercepted_channel(method_name: str, error_attempts: List[int]):
        """
        Creates a gRPC channel with an interceptor for the specified method.

        Args:
            method_name (str): The method to intercept (e.g., "lock_acquire", "lock_release").
            error_attempts (List[int]): The call attempts on which to simulate errors.

        Returns:
            grpc.Channel: An intercepted gRPC channel with simulated error handling for the specified method.
        """
        # Mapping of method names to full gRPC method paths
        method_paths = {
            "lock_acquire": '/lock_service.LockService/lock_acquire',
            "lock_release": '/lock_service.LockService/lock_release',
            "file_append": '/lock_service.LockService/file_append',
            "client_init": '/lock_service.LockService/client_init',
            "client_close": '/lock_service.LockService/client_close'
        }

        # Get the full gRPC method path based on method_name
        full_method_path = method_paths.get(method_name)
        if not full_method_path:
            raise ValueError(f"Unknown method name: {method_name}")

        # Create an intercepted channel with the specified method and error attempts
        intercept_channel = grpc.intercept_channel(
            grpc.insecure_channel('localhost:50051'),
            SimulateErrorInterceptor(full_method_path, error_attempts=error_attempts)
        )

        return intercept_channel

