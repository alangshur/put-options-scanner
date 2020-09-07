from threading import Lock
from copy import deepcopy


class AtomicInteger:

    def __init__(self):
        self.lk = Lock()
        self.integer = 0

    def get(self):
        self.lk.acquire()
        return self.integer
        self.lk.release()

    def fix(self, value):
        self.lk.acquire()
        self.integer = value
        self.lk.release()

    def add(self, value):
        self.lk.acquire()
        self.integer += value
        self.lk.release()

    def subtract(self, value):
        self.lk.acquire()
        self.integer -= value
        self.lk.release()

    def multiply(self, value):
        self.lk.acquire()
        self.integer *= value
        self.lk.release()

    def divide(self, value):
        self.lk.acquire()
        self.integer /= value
        self.lk.release()

    def increment(self):
        self.add(1)
    
    def decrement(self):
        self.subtract(1)


class AtomicList:

    def __init__(self):
        self.lk = Lock()
        self.list = []

    def get(self):
        self.lk.acquire()
        return deepcopy(self.list)
        self.lk.release()

    def size(self):
        self.lk.acquire()
        return len(self.list)
        self.lk.release()

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