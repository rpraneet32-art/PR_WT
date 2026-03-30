import numpy as np
import hashlib

class CountMinSketch:
    def __init__(self, width=500, depth=5):
        self.width = width
        self.depth = depth
        self.table = np.zeros((depth, width))
        self.seeds = [i * 13 + 7 for i in range(depth)]

    def _hash(self, item, seed):
        h = hashlib.md5((str(item) + str(seed)).encode()).hexdigest()
        return int(h, 16) % self.width

    def add(self, item):
        for i in range(self.depth):
            index = self._hash(item, self.seeds[i])
            self.table[i][index] += 1

    def estimate(self, item):
        return min(
            self.table[i][self._hash(item, self.seeds[i])]
            for i in range(self.depth)
        )