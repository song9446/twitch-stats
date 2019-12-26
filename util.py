
def split_into_even_size(lst, size):
    return [lst[i:i + size] for i in range(0, len(lst), size)]

import datetime

class ExpiredSet:
    def __init__(self):
        self.container = {}
    def add(self, val, seconds):
        self.container[val] = datetime.datetime.now() + datetime.timedelta(seconds=seconds)
    def remove(self, val):
        self.container.pop(val)
    def maintain(self):
        now = datetime.datetime.now()
        for k, v in self.container.items():
            if now >= v:
                self.remove(k)
    def intersection(self, another):
        return [key for key in self.to_list() if another.container.get(key)]
    def to_list(self):
        return self.container.keys()

