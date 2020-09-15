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

    def update_min(self, value):
        self.lk.acquire()
        if value < self.integer:
            self.integer = value
        self.lk.release()

    def update_max(self, value):
        self.lk.acquire()
        if value > self.integer:
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


class AtomicMap:

    def __init__(self):
        self.lk = Lock()
        self.map = {}

    def get(self):
        self.lk.acquire()
        temp_map = deepcopy(self.map)
        self.lk.release()
        return temp_map

    def size(self):
        self.lk.acquire()
        temp_size = len(self.map)
        self.lk.release()
        return temp_size

    def update(self, key, value):
        self.lk.acquire()
        self.map[key] = value
        self.lk.release()

    def remove(self, key):
        self.lk.acquire()
        self.map.pop(key, None)
        self.lk.release()


class AtomicNestedMap:

    def __init__(self):
        self.lk = Lock()
        self.map = {}

    def get(self):
        self.lk.acquire()
        temp_map = deepcopy(self.map)
        self.lk.release()
        return temp_map

    def size(self):
        self.lk.acquire()
        temp_size = len(self.map)
        self.lk.release()
        return temp_size

    def nested_size(self, key1):
        self.lk.acquire()
        if key1 in self.map: temp_size = len(self.map[key1])
        else: temp_size = None
        self.lk.release()
        return temp_size

    def update(self, key1, key2, value):
        self.lk.acquire()
        if key1 in self.map: self.map[key1][key2] = value
        else: self.map[key1] = {key2: value}
        self.lk.release()

    def remove(self, key1):
        self.lk.acquire()
        self.map.pop(key1, None)
        self.lk.release()

    def nested_remove(self, key1, key2):
        self.lk.acquire()
        if key1 in self.map: self.map[key1].pop(key2, None)
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