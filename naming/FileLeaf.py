import threading
from threading import Event, Condition
from RWLock import RWLock


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
