"""
hll_wrapper.py
---------------
Wrapper around the `datasketch` library's HyperLogLog implementation.
"""

from datasketch import HyperLogLog
import math

class HLLCounter:
    def __init__(self, precision: int = 14):
        self.precision = max(4, min(16, precision))
        self.hll = HyperLogLog(p=self.precision)
        self.items_added = 0

    def add(self, item):
        self.hll.update(str(item).encode("utf-8"))
        self.items_added += 1

    def estimate_cardinality(self) -> int:
        return int(self.hll.count())

    def get_relative_error(self) -> float:
        return 1.04 / math.sqrt(2 ** self.precision)

    @classmethod
    def from_accuracy_target(cls, accuracy: float = 0.95):
        target_error = 1.0 - accuracy
        if target_error <= 0:
            precision = 16
        else:
            precision = int(math.ceil(2 * math.log2(1.04 / target_error)))
        
        precision = max(4, min(16, precision))
        return cls(precision=precision)