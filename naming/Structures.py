import threading
from threading import Event, Condition


class Registration:
    """
    the class for StorageServer Registration

    **membership**:
        *storage_ip*: storage server IP address.

        *client_port*: storage server port listening for the requests from client (aka storage port).

        *command_port*: storage server port listening for the requests from the naming server.

        *files*: list of files stored on the storage server. This list is merged with the directory tree already present on the naming server. Duplicate filenames are dropped.
    """
    storage_ip = None
    client_port = None
    command_port = None
    files = None

    def __init__(self, storage_ip, client_port, command_port, files=None):
        """
        init function for Registration

        **param** storage_ip: storage server IP address.

        **param** client_port: storage server port listening for the requests from client (aka storage port).

        **param** command_port: storage server port listening for the requests from the naming server.

        **param** files: list of files stored on the storage server. This list is merged with the directory tree already present on the naming server. Duplicate filenames are
        """
        self.storage_ip = storage_ip
        self.client_port = client_port
        self.command_port = command_port
        self.files = files

    def __eq__(self, other):
        """
        rewrite equal function in Registration

        **param** other: an other Registration object

        **return**: `True` if storage_ip,client_port,command_port are the same, `False` if any one elements is not.
        """
        return self.storage_ip == other.storage_ip and self.client_port == other.client_port and self.command_port == other.command_port and self.files == other.files

    def is_different_server(self, other):
        """
        if have different command_port, is different server

        **param** other: an other Registration object

        **return**: `True` if command_port not equals, `False` if command_port equals
        """
        return self.command_port != other.command_port


class LockRequestQueue:
    """
    a Queue-like struture for queueing up lock requests.

    **membership**:
        *shared_counter*:the counter for reads lock requests

        *queue*:the queue queuing reads requests

        *queue_size*:the queue_size
    """
    shared_counter = 0
    queue = list()
    queue_size = 0

    def __init__(self):
        """
        the init function for LockRequestQueue
            ``shared_counter = 0
            queue = []
            queue_size = len(queue) = 0``
        """
        self.shared_counter = 0
        self.queue = list()
        self.queue_size = 0


class ReplicaReport:
    """
    the structure for replication storage

    **membership**:
        *command_ports*: the storages that stores the file

        *replicaed_times*: the times of file replication

        *visited_times*: the cumulated access time for the file

        *is_replicated*: the file is replicated or not
    """
    command_ports = list()
    replicaed_times = 0
    visited_times = 1
    is_replicated = False

    def __init__(self):
        """
        the init function for ReplicaReport

            ``command_ports = []
            replicaed_times = 0
            visited_times = 1
            is_replicated = False``
        """
        command_ports = list()
        replicaed_times = 0
        visited_times = 1
        is_replicated = False


class FileLeaf:
    """
    the lock structure for a file. The Node-like structure that contains the file lock

    **membership**:
        *file_name*: the short file name(not full path)

        *locked*:the fileLeaf is locked or not

        *exclusive*: the lock is exclusive or not

        *filelock*:the Thread lock for lock control
    """
    file_name = None
    locked = False
    exclusive = False
    filelock = None

    def __init__(self, file_name, exclusive=None):
        """
        the init function for FileLeaf, set lock to initial state

        **param** file_name: the short file name

        **param** exclusive: the lock is exclusive or not, default is exclusive which is safer
        """
        self.file_name = file_name
        self.exclusive = exclusive
        self.filelock = Event()

    def acquire(self, exclusive=None):
        """
        acquire the file lock in this FileLeaf

        **param** exclusive: the lock is exclusive or not, default is exclusive which is safer
        """
        self.filelock.clear()
        self.locked = True
        self.exclusive = exclusive

    def release(self):
        """
        release the file lock in this FileLeaf
        """
        self.filelock.set()
        self.locked = False
        self.exclusive = False


class DirLockReport:
    """
    the lock structure for a directory

    **membership**:
        *filelock*: the lock for the directory

        *locked*: the fileLeaf is locked or not

        *exclusive*: the lock is exclusive or not
    """
    filelock = None
    locked = False
    exclusive = False

    def __init__(self, exclusive=None):
        """
        The initialization for DirLockReport

        set lock to initialization, locked to False, exclusive to False
        """
        self.filelock = Event()
        self.locked = False
        self.exclusive = False

    def acquire(self, exclusive=None):
        """
        acquire the directory lock in this directory

        **param** exclusive: the lock is exclusive or not
        """
        self.filelock.clear()
        self.locked = True
        self.exclusive = exclusive

    def release(self):
        """
        release the directory lock in this directory
        """
        self.filelock.set()
        self.locked = False
        self.exclusive = False

    def set_status(self, locked=None, exclusive=None):
        """
        set locked and exclusive for seperate use

        **param** locked: whether the directory is locked

        **param** exclusive: whether the directory lock is exclusive
        """
        self.locked = locked
        self.exclusive = exclusive



"""
************************************ unused ************************************

************************************ unused ************************************
"""

class ClientHost:
    """
    the class for ClientHost information,

    **membership**:
        *storage_ip*: storage server IP address.

        *client_port*: storage server port listening for the requests from client (aka storage port).
    """
    storage_ip = None
    client_port = None

    def __init__(self, storage_ip, client_port):
        """
        **param** storage_ip: storage server IP address.

        **param** client_port: storage server port listening for the requests from client (aka storage port).
        """
        self.storage_ip = storage_ip
        self.client_port = client_port

    def __eq__(self, other):
        """
        two ClientHosts are considered the same if have the same storage_ip and client_port(aka storage port)

        **param** other: the ClientHosts to compared to

        **return**: True if storage_ip and client_port are same.
        """
        return self.storage_ip == other.storage_ip and self.client_port == other.client_port

    def get_storage_ip(self):
        """
        getter for storage_ip

        **return**: storage_ip
        """
        return self.storage_ip

    def get_client_port(self):
        """
        getter for client_port

        **return**: client_port
        """
        return self.client_port

class FileLock:
    """
    FileLock using self-written RWLock
    """
    file_name = None
    is_locked = False
    rwlock = None

    def __init__(self, file_name):
        """
        init for FileLock

        **param** file_name: the name of the file
        """
        self.file_name = file_name
        self.is_locked = False
        self.rwlock = RWLock()

    def read_acquire(self):
        """
        acquire reader lock(aka non-exclusive, shared)

        **return**:
        """
        self.is_locked = True
        self.rwlock = RWLock().read_acquire()

    def read_release(self):
        """
        release reader lock(aka non-exclusive, shared)

        **return**:
        """
        self.is_locked = False
        self.rwlock = RWLock().read_acquire()

    def write_acquire(self):
        """
        acquire writer lock(aka exclusive)

        **return**:
        """
        self.is_locked = True
        self.rwlock = RWLock().write_acquire()

    def write_release(self):
        """
        release writer lock(aka exclusive)

        **return**:
        """
        self.is_locked = False
        self.rwlock = RWLock().write_release()


class RWLock(object):
    """
    self written RWLock using thread condition
    """
    def __init__(self):
        """
        initialize function
        """
        self.lock = threading.Lock()
        self.rcond = threading.Condition(self.lock)
        self.wcond = threading.Condition(self.lock)
        self.read_waiter = 0
        self.write_waiter = 0
        self.state = 0
        self.owners = []
        self.write_first = True

    def write_acquire(self, blocking=True):
        """
        acquire writer lock

        **param** blocking: whether to block or not

        **return**: True of success
        """
        me = threading.get_ident()
        with self.lock:
            while not self._write_acquire(me):
                if not blocking:
                    return False
                self.write_waiter += 1
                self.wcond.wait()
                self.write_waiter -= 1
        return True

    def _write_acquire(self, me):
        if self.state == 0 or (self.state < 0 and me in self.owners):
            self.state -= 1  # set one writer
            self.owners.append(me)
            return True
        if self.state > 0 and me in self.owners:
            raise RuntimeError('cannot recursively wrlock a rdlocked lock')
        return False

    def read_acquire(self, blocking=True):
        """
        acquire reader lock

        **param** blocking: whether to block or not

        **return**: True of success
        """
        me = threading.get_ident()
        with self.lock:
            while not self._read_acquire(me):
                if not blocking:
                    return False
                self.read_waiter += 1
                self.rcond.wait()
                self.read_waiter -= 1
        return True

    def _read_acquire(self, me):
        if self.state < 0:
            # if the lock has already be acquired
            return False

        if not self.write_waiter:
            ok = True
        else:
            ok = me in self.owners
        if ok or not self.write_first:
            self.state += 1
            self.owners.append(me)
            return True
        return False

    def unlock(self):
        """
        unlock oneself
        """
        me = threading.get_ident()
        with self.lock:
            try:
                self.owners.remove(me)
            except ValueError:
                raise RuntimeError('cannot release un-acquired lock')

            if self.state > 0:
                self.state -= 1
            else:
                self.state += 1
            if not self.state:
                if self.write_waiter and self.write_first:  # if write is waiting
                    self.wcond.notify()
                elif self.read_waiter:
                    self.rcond.notify_all()
                elif self.write_waiter:
                    self.wcond.notify()

    def read_release(self):
        """
        unlock reader
        """
        self.unlock(self)

    def write_release(self):
        """
        unlock writer
        """
        self.unlock(self)
