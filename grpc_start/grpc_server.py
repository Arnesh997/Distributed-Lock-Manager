import grpc
from concurrent import futures
import threading
import lock_pb2
import lock_pb2_grpc
import os
from collections import deque
import logging
import time
import pickle
import argparse
from google.protobuf import empty_pb2

# Constants
FILE_DIR = "./server_files"
STATE_FILE = 'server_state.pkl'
HEARTBEAT_INTERVAL = 2  # Seconds between heartbeats
HEARTBEAT_TIMEOUT = 5   # Seconds to wait before considering primary dead
RETRY_DELAY = 1   # Time between we retry to sync with a server in case of failure
SYNC_MAX_TIMEOUT = 10  # Maximum time for s server replica to restart before declaring dead


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class ServerState:
    def __init__(self, server_id, servers):
        self.server_id = server_id  # Should be an integer(for comparision in Bully Algo)
        self.is_primary = False
        self.primary_id = None
        self.last_heartbeat = None  # Time of last heartbeat received from primary
        self.servers = servers  # dict of server_id to address
        self._lock = threading.Lock()
        self.election_in_progress = False
        self.dead_replicas = set()

class DistributedLock:
    def __init__(self):
        self._lock = threading.Lock()
        self._holder = None
        self._wait_queue = deque()  # Queue of client requests
        self._condition = threading.Condition(self._lock)
        self._holder_deadline = None    # Last possible time for a lock holder(Useful when server is changed to ensure constant hold time)
        self.lock_timeout = 5

    def acquire(self, client_id):
        logging.info(f"Client {client_id} attempting to acquire lock")
        with self._lock:
            now = time.time()
            if self._holder is None or (self._holder_deadline and now > self._holder_deadline):
                self._holder = client_id
                self._holder_deadline = now + self.lock_timeout
                logging.info(f"Lock granted to client {client_id}")
                return True
            elif self._holder == client_id:
                logging.info(f"Client {client_id} already holds the lock")
                return True
            else:
                logging.info(f"Client {client_id} added to wait queue")
                self._wait_queue.append(client_id)
                while self._holder != client_id:
                    self._condition.wait()
                logging.info(f"Lock granted to waiting client {client_id}")
                return True

    def release(self, client_id):
        logging.info(f"Client {client_id} attempting to release lock")
        with self._lock:
            if self._holder != client_id:
                logging.warning(f"Client {client_id} attempted to release lock they don't hold")
                return False
            self._holder = None
            self._holder_deadline = None
            if self._wait_queue:
                self._holder = self._wait_queue.popleft()
                self._holder_deadline = time.time() + self.lock_timeout
                self._condition.notify_all()
                logging.info(f"Lock passed to waiting client {self._holder}")
            else:
                logging.info("Lock released with no waiting clients")
            return True

    def get_holder(self):
        with self._lock:
            return self._holder

class LockServiceServicer(lock_pb2_grpc.LockServiceServicer):
    def __init__(self, server_state):
        self.distributed_lock = DistributedLock()
        self.server_state = server_state
        self.files = {f"file_{i}": os.path.join(FILE_DIR, f"file_{i}.txt") for i in range(100)}
        self.processed_requests = {}
        self.heartbeat_thread = None

        if not os.path.exists(FILE_DIR):
            os.makedirs(FILE_DIR)
        for file_name in self.files.values():
            if not os.path.exists(file_name):
                with open(file_name, 'w') as f:
                    f.write("")

        self.load_state()

        if self.server_state.is_primary:
            self.start_heartbeat_thread()
        else:
            self.monitor_primary()

    def save_state(self):
        state = self.get_current_state()
        try:
            serialized_state = pickle.dumps(state)
            with open(STATE_FILE, 'wb') as f:
                f.write(serialized_state)
            logging.info("Server state saved to disk")
        except Exception as e:
            logging.error(f"Error saving state: {e}")
            logging.error(f"State: {state}")

    def load_state(self):
        if os.path.exists(STATE_FILE):
            try:
                with open(STATE_FILE, 'rb') as f:
                    state = pickle.load(f)
                self.apply_state(state)
                logging.info("Server state loaded from disk")
            except Exception as e:
                logging.error(f"Error loading state: {e}")

    def start_heartbeat_thread(self):
        """Start the heartbeat thread to send periodic heartbeats to replicas."""
        if self.heartbeat_thread and self.heartbeat_thread.is_alive():
            logging.info("Heartbeat thread is already running")
            return

        def send_heartbeats():
            try:
                while self.server_state.is_primary:  # Only send heartbeats while primary
                    self.broadcast_heartbeat()
                    time.sleep(HEARTBEAT_INTERVAL)
            except Exception as e:
                logging.error(f"Error in heartbeat thread: {e}")

        self.heartbeat_thread = threading.Thread(target=send_heartbeats, daemon=True)
        self.heartbeat_thread.start()
        logging.info("Started heartbeat thread as primary")

    def broadcast_heartbeat(self):
        request = lock_pb2.HeartbeatRequest(
            server_id=self.server_state.server_id,
            is_primary=True,
            timestamp=int(time.time())
        )
        for sid, addr in self.server_state.servers.items():
            if sid != self.server_state.server_id:
                try:
                    channel = grpc.insecure_channel(
                        addr,
                        options=[
                            ('grpc.keepalive_time_ms', 10000),
                            ('grpc.keepalive_timeout_ms', 5000),
                            ('grpc.keepalive_permit_without_calls', True),
                            ('grpc.http2.max_pings_without_data', 0)
                        ]
                    )
                    stub = lock_pb2_grpc.LockServiceStub(channel)
                    response = stub.SendHeartbeat(request, timeout=2)
                    if sid in self.server_state.dead_replicas and response.acknowledged:
                        logging.info(f"Replica {sid} with address {addr} has come back online")
                        with self.server_state._lock:
                            self.server_state.dead_replicas.remove(sid)
                        logging.info(f"Removed {sid} from dead_replica set. Current dead replica set: {self.server_state.dead_replicas}")
                        threading.Thread(target= self.sync_with_replica, args=(sid, addr), daemon=True).start()

                except Exception as e:
                    logging.warning(f"Failed to send heartbeat to server {sid}")
                    with self.server_state._lock:
                        self.server_state.dead_replicas.add(sid)
                    
                finally:
                    channel.close()
    
    # Replica method
    def SendHeartbeat(self, request, context):
        with self.server_state._lock:
            if request.is_primary:
                self.server_state.last_heartbeat = time.time()
                self.server_state.primary_id = request.server_id
        logging.info(f"Heartbeat received from server {request.server_id}")

        return lock_pb2.HeartbeatResponse(
            acknowledged=True,
            server_id=self.server_state.server_id
        )

    # Replica method
    def monitor_primary(self):
        """Monitor primary's health and initiate failover if necessary."""
        def check_primary_health():
            try:
                last_failed_heartbeat = None
                while True:
                    with self.server_state._lock:
                        is_primary = self.server_state.is_primary
                        last_heartbeat = self.server_state.last_heartbeat
                        primary_id = self.server_state.primary_id

                    if is_primary:
                        # If we're primary, no need to monitor
                        time.sleep(1)
                        continue

                    current_time = time.time()

                    if last_heartbeat is None:
                        if last_failed_heartbeat is None:
                            last_failed_heartbeat = current_time
                            logging.info("No heartbeat received yet, starting failure timer")
                        elif current_time - last_failed_heartbeat > HEARTBEAT_TIMEOUT:
                            logging.warning("Never received initial heartbeat, initiating election")
                            self.initiate_election()
                            last_failed_heartbeat = None
                    elif current_time - last_heartbeat > HEARTBEAT_TIMEOUT:
                        if last_failed_heartbeat is None:
                            last_failed_heartbeat = current_time
                            logging.warning(f"No heartbeat for {HEARTBEAT_TIMEOUT} seconds")
                        elif current_time - last_failed_heartbeat > HEARTBEAT_TIMEOUT:
                            logging.warning("Primary confirmed dead, initiating election")
                            self.initiate_election()
                            last_failed_heartbeat = None
                    else:
                        if last_failed_heartbeat is not None:
                            logging.info("Heartbeat received, resetting failure timer")
                        last_failed_heartbeat = None

                    time.sleep(1)
            except Exception as e:
                logging.error(f"Error in primary monitoring thread: {e}")

        monitor_thread = threading.Thread(target=check_primary_health, daemon=True)
        monitor_thread.start()
        logging.info("Started primary monitoring thread")

    def initiate_election(self):
        with self.server_state._lock:
            if self.server_state.election_in_progress or self.server_state.is_primary:
                return
            self.server_state.election_in_progress = True

        logging.info("Initiating election...")
        higher_servers = {sid: addr for sid, addr in self.server_state.servers.items() if sid > self.server_state.server_id}

        responses = []

        for sid, addr in higher_servers.items():
            try:
                channel = grpc.insecure_channel(addr)
                stub = lock_pb2_grpc.LockServiceStub(channel)
                response = stub.Election(lock_pb2.ElectionRequest(server_id=self.server_state.server_id), timeout=2)
                if response.ok:
                    responses.append(sid)
                    logging.info(f"Received OK from server {sid}")
            except Exception as e:
                logging.error(f"Failed to contact server {sid} during election")
            finally:
                channel.close()

        if not responses:
            # No higher servers responded, become primary
            logging.info("No higher server responded. Becoming primary.")
            with self.server_state._lock:
                self.server_state.is_primary = True
                self.server_state.primary_id = self.server_state.server_id
                self.server_state.election_in_progress = False
            self.start_heartbeat_thread()
            self.announce_coordinator()
        else:
            # Wait for new coordinator to be announced
            logging.info("Waiting for higher server to become primary.")
            with self.server_state._lock:
                self.server_state.election_in_progress = False

    def Election(self, request, context):
        logging.info(f"Received election message from server {request.server_id}")
        if self.server_state.server_id > request.server_id:
            # Respond and start own election
            threading.Thread(target=self.initiate_election, daemon=True).start()
            return lock_pb2.ElectionResponse(ok=True)
        else:
            # Do not respond
            return lock_pb2.ElectionResponse(ok=False)

    def announce_coordinator(self):
        notification = lock_pb2.CoordinatorNotification(server_id=self.server_state.server_id)
        for sid, addr in self.server_state.servers.items():
            if sid != self.server_state.server_id:
                try:
                    channel = grpc.insecure_channel(addr)
                    stub = lock_pb2_grpc.LockServiceStub(channel)
                    stub.Coordinator(notification, timeout=2)
                    logging.info(f"Announced self as coordinator to server {sid}")
                except Exception as e:
                    logging.error(f"Failed to announce coordinator to server {sid}")
                finally:
                    channel.close()

    def Coordinator(self, request, context):
        with self.server_state._lock:
            self.server_state.is_primary = False
            self.server_state.primary_id = request.server_id
        logging.info(f"New primary announced: Server {request.server_id}")
        return empty_pb2.Empty()

    def SyncState(self, request, context):
        """Handle incoming state sync request."""
        if self.server_state.is_primary:
            return lock_pb2.Response(status=lock_pb2.Status.NOT_PRIMARY)
        try:
            state = pickle.loads(request.state)
            self.apply_state(state)
            return lock_pb2.Response(status=lock_pb2.Status.SUCCESS)
        except Exception as e:
            logging.error(f"Error applying synced state: {e}")
            return lock_pb2.Response(status=lock_pb2.Status.SYNC_ERROR)

    # Modify the sync_with_replicas method

    def sync_with_replicas(self):
        if not self.server_state.is_primary:
            return

        state = self.get_current_state()
        sync_request = lock_pb2.StateSync(
            state=pickle.dumps(state),
            timestamp=int(time.time()),
            primary_id=self.server_state.server_id
        )

        for sid, addr in self.server_state.servers.items():
            if sid != self.server_state.server_id and sid not in self.server_state.dead_replicas:
                start_time = time.time()
                success = False

                while time.time() - start_time < SYNC_MAX_TIMEOUT:
                    try:
                        with grpc.insecure_channel(
                            addr,
                            options=[
                                ('grpc.keepalive_time_ms', 10000),
                                ('grpc.keepalive_timeout_ms', 5000),
                                ('grpc.keepalive_permit_without_calls', True),
                                ('grpc.http2.max_pings_without_data', 0)
                            ]
                        ) as channel:
                            stub = lock_pb2_grpc.LockServiceStub(channel)
                            response = stub.SyncState(sync_request, timeout=HEARTBEAT_TIMEOUT)
                            if response.status == lock_pb2.Status.SUCCESS:
                                logging.info(f"Successfully synced with replica {sid} at address {addr}")
                                success = True
                                break
                            else:
                                logging.warning(f"Failed to sync with replica {sid}: {response.message}")
                    except grpc.RpcError as e:
                        if e.code() == grpc.StatusCode.DEADLINE_EXCEEDED:
                            logging.warning(f"Timeout while syncing with replica {sid}. Retrying...")
                        else:
                            logging.warning(f"RPC error syncing with replica {sid}: Retrying...")
                    except Exception as e:
                        logging.warning(f"Unexpected error syncing with replica {sid}: Retrying...")

                    # Wait before retrying
                    time.sleep(RETRY_DELAY)

                if not success:
                    logging.warning(f"Could not sync with replica {sid} after {SYNC_MAX_TIMEOUT} seconds. Declaring it as dead.")
                    self.server_state.dead_replicas.add(sid)
                    logging.info(f"Dead replica node list {self.server_state.dead_replicas}")

    def sync_with_replica(self, sid, addr):
        """Synchronize state with a single replica that has come back online."""
        if not self.server_state.is_primary:
            return

        state = self.get_current_state()
        sync_request = lock_pb2.StateSync(
            state=pickle.dumps(state),
            timestamp=int(time.time()),
            primary_id=self.server_state.server_id
        )

        try:
            with grpc.insecure_channel(
                addr,
                options=[
                    ('grpc.keepalive_time_ms', 10000),
                    ('grpc.keepalive_timeout_ms', 5000),
                    ('grpc.keepalive_permit_without_calls', True),
                    ('grpc.http2.max_pings_without_data', 0)
                ]
            ) as channel:
                stub = lock_pb2_grpc.LockServiceStub(channel)
                response = stub.SyncState(sync_request, timeout=HEARTBEAT_TIMEOUT)
                if response.status == lock_pb2.Status.SUCCESS:
                    logging.info(f"Successfully synced with recovered replica {sid} at address {addr}")
                else:
                    logging.warning(f"Failed to sync with recovered replica {sid}: {response.message}")
        except Exception as e:
            logging.warning(f"Error syncing with recovered replica {sid}: {e}")
            # Optionally, re-add the replica to dead_replicas if sync fails
            with self.server_state._lock:
                self.server_state.dead_replicas.add(sid)


    def get_current_state(self):
        return {
            'lock_holder': self.distributed_lock._holder,
            'lock_holder_deadline': self.distributed_lock._holder_deadline,
            'wait_queue': list(self.distributed_lock._wait_queue),
            'processed_requests': self.processed_requests,
            'files_content': self.get_files_content()
        }

    def get_files_content(self):
        files_content = {}
        for file_name, file_path in self.files.items():
            try:
                with open(file_path, 'r') as f:
                    files_content[file_name] = f.read()
            except Exception as e:
                logging.error(f"Error reading file {file_name}: {e}")
        return files_content

    def apply_state(self, state):
        with self.distributed_lock._lock:
            self.distributed_lock._holder = state['lock_holder']
            self.distributed_lock._holder_deadline = state['lock_holder_deadline']
            self.distributed_lock._wait_queue = deque(state['wait_queue'])
            self.processed_requests = state['processed_requests']
            for file_name, content in state['files_content'].items():
                file_path = self.files.get(file_name)
                if file_path:
                    with open(file_path, 'w') as f:
                        f.write(content)

    def client_init(self, request, context):
        if self.server_state.is_primary:
            # -1 is for testing purpose to ensure connection is with primary only
            if request.rc != -1:
                logging.info(f"Initializing client with ID: {request.rc}")
            return lock_pb2.Response(
                status=lock_pb2.Status.SUCCESS,
                message=f"Client {request.rc} initialized"
            )
        else:
            return lock_pb2.Response(
                status=lock_pb2.Status.NOT_PRIMARY,
                message=f"Server is not primary"
            )

    def lock_acquire(self, request, context):
        if not self.server_state.is_primary:
            return lock_pb2.Response(
                status=lock_pb2.Status.NOT_PRIMARY,
                message="This is a replica server. Please connect to the primary."
            )
        try:
            acquired = self.distributed_lock.acquire(request.client_id)
            if acquired:
                self.save_state()
                threading.Thread(target=self.sync_with_replicas, daemon=True).start() 
                return lock_pb2.Response(status=lock_pb2.Status.SUCCESS)
            return lock_pb2.Response(status=lock_pb2.Status.LOCK_ACQUIRE_ERROR)
        except Exception as e:
            logging.error(f"Error during lock acquisition: {e}")
            return lock_pb2.Response(status=lock_pb2.Status.LOCK_ACQUIRE_ERROR)

    def lock_release(self, request, context):
        if not self.server_state.is_primary:
            return lock_pb2.Response(
                status=lock_pb2.Status.NOT_PRIMARY,
                message="This is a replica server. Please connect to the primary."
            )
        try:
            released = self.distributed_lock.release(request.client_id)
            if released:
                self.save_state()
                threading.Thread(target=self.sync_with_replicas, daemon=True).start()
                return lock_pb2.Response(status=lock_pb2.Status.SUCCESS)
            return lock_pb2.Response(status=lock_pb2.Status.LOCK_RELEASE_ERROR)
        except Exception as e:
            logging.error(f"Error during lock release: {e}")
            return lock_pb2.Response(status=lock_pb2.Status.LOCK_RELEASE_ERROR)

    def file_append(self, request, context):
        if not self.server_state.is_primary:
            return lock_pb2.Response(
                status=lock_pb2.Status.NOT_PRIMARY,
                message="This is a replica server. Please connect to the primary."
            )
        if request.request_id in self.processed_requests:
            # Reconstruct the response from stored data
            response_data = self.processed_requests[request.request_id]
            return lock_pb2.Response(
                status=response_data['status'],
                message=response_data.get('message', '')
            )
        current_holder = self.distributed_lock.get_holder()
        if current_holder != request.client_id:
            response = lock_pb2.Response(
                status=lock_pb2.Status.FILE_ERROR,
                message="You don't hold the lock"
            )
            # Store a serializable representation
            self.processed_requests[request.request_id] = {
                'status': response.status,
                'message': response.message
            }
            return response
        try:
            if request.filename in self.files:
                with open(self.files[request.filename], 'a') as f:
                    f.write(request.content.decode('utf-8') + "\n")
                response = lock_pb2.Response(status=lock_pb2.Status.SUCCESS)
                # Store a serializable representation
                self.processed_requests[request.request_id] = {
                    'status': response.status,
                    'message': response.message
                }
                self.save_state()
                threading.Thread(target=self.sync_with_replicas, daemon=True).start()
                return response
            return lock_pb2.Response(status=lock_pb2.Status.FILE_ERROR)
        except Exception as e:
            logging.error(f"Error in file_append: {e}")
            return lock_pb2.Response(status=lock_pb2.Status.FILE_ERROR)

    def client_close(self, request, context):
        if not self.server_state.is_primary:
            return lock_pb2.Response(
                status=lock_pb2.Status.NOT_PRIMARY,
                message="This is a replica server. Please connect to the primary."
            )
        current_holder = self.distributed_lock.get_holder()
        if current_holder == request.rc:
            self.distributed_lock.release(request.rc)
        logging.info(f"Closing client with ID: {request.rc}")
        return lock_pb2.Response(status=lock_pb2.Status.SUCCESS)

def serve(server_id, port, servers):
    server_state = ServerState(server_id, servers)
    # server_state.is_primary = (server_id == max(servers.keys()))
    server_state.is_primary = False
    server_state.primary_id = None
    server = grpc.server(
        futures.ThreadPoolExecutor(max_workers=10),
        options=[
            ('grpc.keepalive_time_ms', 10000),
            ('grpc.keepalive_timeout_ms', 5000),
            ('grpc.keepalive_permit_without_calls', True),
            ('grpc.http2.max_pings_without_data', 0),
            ('grpc.max_connection_idle_ms', 30000),
            ('grpc.max_connection_age_ms', 300000),
        ]
    )
    servicer = LockServiceServicer(server_state)
    lock_pb2_grpc.add_LockServiceServicer_to_server(servicer, server)
    server.add_insecure_port(f'[::]:{port}')
    server.start()
    role = 'primary' if server_state.is_primary else 'replica'
    logging.info(f"Server {server_id} starting on port {port} as {role}")
    try:
        server.wait_for_termination()
    except KeyboardInterrupt:
        logging.info("Shutting down server gracefully...")
        server.stop(5)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Start a lock service server')
    parser.add_argument('--server_id', type=int, required=True, help='Unique ID of this server')
    parser.add_argument('--port', type=str, required=True, help='Port to run the server on')
    parser.add_argument('--servers', type=str, nargs='+', required=True,
                        help='List of server IDs and addresses in format id:host:port')
    args = parser.parse_args()

    servers = {}
    for s in args.servers:
        parts = s.split(':')
        if len(parts) != 3:
            logging.error(f"Invalid server specification: {s}")
            exit(1)
        sid = int(parts[0])
        addr = f"{parts[1]}:{parts[2]}"
        servers[sid] = addr

    logging.info(f"Starting server {args.server_id} on port {args.port} with servers: {servers}")

    serve(args.server_id, args.port, servers)
