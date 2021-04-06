import threading
from threading import Event, Condition
from RWLock import RWLock
class FileLeaf:
    file_name = None
    locked = False
    exclusive = False
    mutex = None

    def __init__(self, file_name, exclusive=None):
        self.file_name = file_name
        self.exclusive = exclusive
        self.mutex = Event()

