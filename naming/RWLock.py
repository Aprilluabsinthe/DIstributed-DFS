import threading


class RWLock(object):
    def __init__(self):
        self.lock = threading.Lock()
        self.rcond = threading.Condition(self.lock)
        self.wcond = threading.Condition(self.lock)
        self.read_waiter = 0  # queueing readers
        self.write_waiter = 0  # queueing writers
        self.state = 0  # positive: readers, negetive:writers(-1)
        self.owners = []  # thread id collections
        self.write_first = True  # write first or read first

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