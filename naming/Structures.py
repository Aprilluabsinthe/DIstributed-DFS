import threading
from threading import Event, Condition


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

    def is_different_server(self, other):
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


class FileLeaf:
    file_name = None
    locked = False
    exclusive = False
    filelock = None

    def __init__(self, file_name, exclusive=None):
        self.file_name = file_name
        self.exclusive = exclusive
        self.filelock = Event()

    def acquire(self, file_name, exclusive=None):
        self.filelock.clear()
        self.locked = True
        self.exclusive = exclusive

    def release(self):
        self.filelock.set()
        self.locked = False
        self.exclusive = False


class DirLockReport:
    filelock = None
    locked = False
    exclusive = False

    def __init__(self, exclusive=None):
        self.filelock = Event()
        self.locked = False
        self.exclusive = False

    def acquire(self, exclusive=None):
        self.filelock.clear()
        self.locked = True
        self.exclusive = exclusive

    def release(self):
        self.filelock.set()
        self.locked = False
        self.exclusive = False

    def set_status(self, locked=None, exclusive=None):
        self.locked = locked
        self.exclusive = exclusive



"""
************************************ unused ************************************

************************************ unused ************************************
"""

class FileLock:
    file_name = None
    is_locked = False
    rwlock = None

    def __init__(self, file_name):
        self.file_name = file_name
        self.is_locked = False
        self.rwlock = RWLock()

    def read_acquire(self):
        self.is_locked = True
        self.rwlock = RWLock().read_acquire()

    def read_release(self):
        self.is_locked = False
        self.rwlock = RWLock().read_acquire()

    def write_acquire(self):
        self.is_locked = True
        self.rwlock = RWLock().write_acquire()

    def write_release(self):
        self.is_locked = False
        self.rwlock = RWLock().write_release()


class RWLock(object):
    def __init__(self):
        self.lock = threading.Lock()
        self.rcond = threading.Condition(self.lock)
        self.wcond = threading.Condition(self.lock)
        self.read_waiter = 0
        self.write_waiter = 0
        self.state = 0
        self.owners = []
        self.write_first = True

    def write_acquire(self, blocking=True):
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
        # the lock is not used or this thread has own the lock
        if self.state == 0 or (self.state < 0 and me in self.owners):
            self.state -= 1  # set one writer
            self.owners.append(me)
            return True
        if self.state > 0 and me in self.owners:
            raise RuntimeError('cannot recursively wrlock a rdlocked lock')
        return False

    def read_acquire(self, blocking=True):
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
        self.unlock(self)

    def write_release(self):
        self.unlock(self)
