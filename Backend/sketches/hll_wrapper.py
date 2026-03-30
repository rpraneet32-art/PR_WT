from datasketch import HyperLogLog

def hll_count_distinct(values, precision=10):
    """
    Approximate COUNT DISTINCT using HyperLogLog
    precision: higher = more accuracy (4–16 recommended)
    """
    hll = HyperLogLog(p=precision)

    for val in values:
        hll.update(str(val).encode())

    return int(hll.count())