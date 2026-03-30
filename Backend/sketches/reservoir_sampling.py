import pandas as pd

def reservoir_sample(df, k):
    """
    Optimized Sampling: Replaces the slow Python 'for' loop 
    with highly optimized Pandas vectorized sampling.
    """
    n = len(df)
    if k >= n:
        return df
    
    # df.sample uses fast PRNG under the hood, acting as an instant reservoir
    return df.sample(n=k)