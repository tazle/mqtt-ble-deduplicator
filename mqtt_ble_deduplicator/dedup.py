import collections

class Deduplicator(object):
    def __init__(self, max_size=50):
        self.max_size = max_size
        self.buffer = collections.deque(maxlen=max_size)
        self.checker = collections.Counter()

    def add(self, data):
        if len(self.buffer) == self.max_size:
            removed = self.buffer.popleft()
            self.checker[removed] -= 1
        self.buffer.append(data)
        self.checker[data] += 1

    def check(self, data):
        return self.checker[data] > 0

    def __str__(self):
        return "Dedup(buffer: " + str(self.buffer) + ", checker: " + str(self.checker) + ")"
