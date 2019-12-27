
def split_into_even_size(lst, size):
    return [lst[i:i + size] for i in range(0, len(lst), size)]

import datetime

class ExpiredSet:
    def __init__(self, seconds):
        self.container = {}
        self.default_seconds = seconds
    def add(self, val, seconds=None):
        if seconds is None:
            seconds = self.default_seconds
        self.container[val] = datetime.datetime.now() + datetime.timedelta(seconds=seconds)
    def remove(self, val):
        self.container.pop(val)
    def length(self):
        return len(self.container)
    def maintain(self):
        now = datetime.datetime.now()
        for k, v in self.container.items():
            if now >= v:
                self.remove(k)
    def intersection(self, another):
        return [key for key in self.to_list() if another.container.get(key)]
    def to_list(self):
        return self.container.keys()

