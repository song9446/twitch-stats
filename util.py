import asyncio
from collections import deque

def split_into_even_size(lst, size):
    return [lst[i:i + size] for i in range(0, len(lst), size)]

import datetime

class ExpiredSet:
    def __init__(self, seconds, max_seconds=None):
        self.container = {}
        self.max_seconds = max_seconds
    def add(self, val, seconds):
        duration = datetime.timedelta(seconds=seconds)
        expire_at = self.container.get(val)
        if expire_at and expire_at > datetime.datetime.now():
            duration += expire_at - datetime.datetime.now()
        if self.max_seconds: 
            max_duration = datetime.timedelta(seconds=max_seconds)
            if duration > max_duration:
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
    def intersection(self, other):
        return [key for key in list(self.container.keys()) if other.container.get(key)]
    def cosine_similarity(self, other):
        '''
        molecular = sum(self.container[k]*other.container[k] for k in self.container.keys() if k in other.container)
        if not molecular:
            return 0
        denominator = math.sqrt(sum(i*i for i in self.container.values())) * math.sqrt(sum(i*i for i in other.container.values()))
        return molecular / denominator
        '''
    def jaccard_similarity(self, other):
        n = len(self.intersection(other))
        if not n: 
            return 0
        return n / (len(self.container) + len(other.container) - n)
    def to_list(self):
        return list(self.container.keys())

class MergedStream:
    '''
    if iterator yield None value,
    it assume that the iterator is terminated
    and ignore the result of None
    '''
    def __init__(self, size=0, *iterables):
        self._iterables = list(iterables)
        self._wakeup = asyncio.Event()
        self._available_switch = asyncio.Event()
        self._available_switch.set()
        self.done = deque()
        self.next_futs = {}
        self.size = size
    def _add_iters(self, next_futs, on_done):
        while self._iterables:
            it = self._iterables.pop()
            it = it.__aiter__()
            nfut = asyncio.ensure_future(it.__anext__())
            nfut.add_done_callback(on_done)
            next_futs[nfut] = it
            if self.size and len(next_futs) >= self.size:
                self._available_switch.clear()
        return next_futs
    async def __aiter__(self):
        done = self.done
        next_futs = self.next_futs
        def on_done(nfut):
            self.done.append((nfut, next_futs.pop(nfut)))
            if len(next_futs) < self.size:
                self._available_switch.set()
            #done[nfut] = next_futs.pop(nfut)
            self._wakeup.set()
        self._add_iters(next_futs, on_done)
        try:
            while next_futs or self.done:
                await self._wakeup.wait()
                self._wakeup.clear()
                while self.done:
                    nfut, it = self.done.popleft()
                    try:
                        ret = nfut.result()
                    except StopAsyncIteration:
                        continue
                    if ret is None:
                        continue
                    if it:
                        self._iterables.append(it)
                    yield ret
                if self._iterables:
                    self._add_iters(next_futs, on_done)
        finally:
            # if the generator exits with an exception, or if the caller stops
            # iterating, make sure our callbacks are removed
            for nfut in next_futs:
                nfut.remove_done_callback(on_done)
    def append(self, new_iter):
        self._iterables.append(new_iter)
        self._wakeup.set()
    async def available(self):
        await self._available_switch.wait()
    def append_future(self, new_future):
        def on_done(nfut):
            self.done.append((nfut, self.next_futs.pop(nfut)))
            if len(self.next_futs) < self.size:
                self._available_switch.set()
            self._wakeup.set()
        nfut = asyncio.ensure_future(new_future)
        nfut.add_done_callback(on_done)
        self.next_futs[nfut] = None
        if self.size and len(self.next_futs) >= self.size:
            self._available_switch.clear()

async def wrapper(coru, semaphore, sec):
    async with semaphore:
        r = await coru
        await asyncio.sleep(sec)
        return r
def limited_api(n, sec, corus):
    semaphore = asyncio.Semaphore(n)
    return asyncio.gather(*[wrapper(coru, semaphore, sec) for coru in corus])

async def test():
    async def wait(n):
        await asyncio.sleep(n)
        if n == 2:
            print(2)
            return None
        return n
    m = MergedStream(2)
    m.append_future(wait(1))
    m.append_future(wait(2))
    m.append_future(wait(3))
    async for i in m:
        await m.available()
        print(i)
        m.append_future(wait(3))
        m.append_future(wait(2))
        m.append_future(wait(1))

if __name__ == "__main__":
    import time
    asyncio.run(test())
    a = ExpiredSet(3)
    a.add(0, 3)
    a.add(1, 5)
    b = ExpiredSet(3)
    b.add(0, 2)
    b.add(3, 3)
    print(a.jaccard_similarity(b))
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
    
