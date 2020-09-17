from threading import Lock
from copy import deepcopy


class AtomicInteger:

    def __init__(self, value=0):
        self.lk = Lock()
        self.integer = value

    def get(self):
        self.lk.acquire()
        temp_int = self.integer
        self.lk.release()
        return temp_int

    def update(self, value):
        self.lk.acquire()
        self.integer = value
        self.lk.release()

    def add(self, value):
        self.lk.acquire()
        self.integer += value
        self.lk.release()

    def multiply(self, value):
        self.lk.acquire()
        self.integer *= value
        self.lk.release()


class AtomicList:

    def __init__(self):
        self.lk = Lock()
        self.list = []

    def get(self):
        self.lk.acquire()
        temp_list = deepcopy(self.list)
        self.lk.release()
        return temp_list

    def size(self):
        self.lk.acquire()
        temp_size = len(self.list)
        self.lk.release()
        return temp_size

    def append(self, value):
        self.lk.acquire()
        self.list.append(value)
        self.lk.release()

    def remove(self, value):
        self.lk.acquire()
        self.list.remove(value)
        self.lk.release()

    def insert(self, index, value):
        self.lk.acquire()
        self.list.insert(index, value)
        self.lk.release()


class AtomicBool:

    def __init__(self, value=False):
        self.lk = Lock()
        self.bool = value

    def get(self):
        self.lk.acquire()
        temp_bool = self.bool
        self.lk.release()
        return temp_bool
    
    def update(self, value):
        self.lk.acquire()
        self.bool = value
        self.lk.release()

    def flip(self, value):
        self.lk.acquire()
        self.bool = not self.bool
        self.lk.release()