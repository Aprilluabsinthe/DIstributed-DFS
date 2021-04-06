import threading
from threading import Event, Condition

from naming.RWLock import RWLock

class FileBranch:
    file_name = None
    is_locked = False
    is_writer = False
    mutex = None

    def __init__(self, file_name, is_writer=None):
        self.file_name = file_name
        self.is_writer = is_writer
        self.mutex = RWLock()