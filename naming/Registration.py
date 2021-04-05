class Registration:
    storage_ip = None
    client_port = None
    command_port = None
    files = None

    def __init__(self, storage_ip, client_port, command_port, files):
        self.storage_ip = storage_ip
        self.client_port = client_port
        self.command_port = command_port
        self.files = files

    def __eq__(self, other):
        return self.storage_ip == other.storage_ip and self.client_port == other.client_port and self.command_port == other.command_port and self.files == other.files


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
