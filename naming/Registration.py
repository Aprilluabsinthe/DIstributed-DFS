class Registration:
    storage_ip = None
    client_port = None
    command_port = None
    files = None

    def __init__(self, storage_ip, client_port, command_port, files=None):
        self.storage_ip = storage_ip
        self.client_port = client_port
        self.command_port = command_port
        self.files = files

    def __eq__(self, other):
        return self.storage_ip == other.storage_ip and self.client_port == other.client_port and self.command_port == other.command_port and self.files == other.files

    def is_different_server(self,other):
        return self.command_port != other.command_port

class ClientHost:
    storage_ip = None
    client_port = None

    def __init__(self, storage_ip, client_port):
        self.storage_ip = storage_ip
        self.client_port = client_port

    def __eq__(self, other):
        return self.storage_ip == other.storage_ip and self.client_port == other.client_port

    def get_storage_ip(self):
        return self.storage_ip

    def get_client_port(self):
        return self.client_port


class LockRequestQueue:
    shared_counter = 0
    queue = list()
    queue_size = 0

    def __init__(self):
        self.shared_counter = 0
        self.queue = list()
        self.queue_size = 0


class ReplicaReport:
    command_ports = list()
    replicaed_times = 0
    visited_times = 1
    is_replicated = False

    def __init__(self):
        command_ports = list()
        replicaed_times = 0
        visited_times = 1
        is_replicated = False
