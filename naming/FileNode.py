from threading import Event
class FileNode:
    file_name = None
    is_locked = False

    def __init__(self, file_name):
        self.file_name = file_name
        self.is_locked = False