
def split_into_even_size(lst, size):
    return [lst[i:i + size] for i in range(0, len(lst), size)]

import datetime

class ExpiredSet:
    def __init__(self, seconds):
        self.container = {}
        self.default_seconds = seconds
    def add(self, val, seconds=None, max_seconds=None):
        if seconds is None:
            seconds = self.default_seconds
        duration = datetime.timedelta(seconds=seconds)
        expire_at = self.container.get(val)
        if expire_at and expire_at > datetime.datetime.now():
            duration += expire_at - datetime.datetime.now()
        if max_seconds: max_duration = datetime.timedelta(seconds=max_seconds)
        if max_seconds and duration > max_duration:
            duration = max_duration
        self.container[val] = datetime.datetime.now() + duration
    def remove(self, val):
        self.container.pop(val)
    def length(self):
        return len(self.container)
    def maintain(self):
        now = datetime.datetime.now()
        for k, v in list(self.container.items()):
            if now >= v:
                self.remove(k)
    def intersection(self, another):
        return [key for key in self.to_list() if another.container.get(key)]
    def to_list(self):
        return list(self.container.keys())

if __name__ == "__main__":
    import time
    a = ExpiredSet(3)
    a.add(0, 3)
    a.add(1, 5)
    a.maintain()
    assert(a.to_list() == [0,1])
    assert(a.length() == 2)
    time.sleep(2)
    a.maintain()
    assert(a.to_list() == [0,1])
    assert(a.length() == 2)
    time.sleep(2)
    a.maintain()
    assert(a.to_list() == [1])
    assert(a.length() == 1)
    a.add(1, 2)
    time.sleep(2)
    a.maintain()
    assert(a.to_list() == [1])
    assert(a.length() == 1)
    time.sleep(2)
    a.maintain()
    assert(a.to_list() == [])
    assert(a.length() == 0)
    
