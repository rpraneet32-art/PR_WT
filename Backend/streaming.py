"""
streaming.py
-------------
Real-time streaming analytics engine.
"""

import asyncio
import time
import numpy as np
from typing import Dict, Any

# --- PATH FIX: Direct import from sketches folder ---
from sketches.count_min_sketch import CountMinSketch
from sketches.hll_wrapper import HLLCounter

CATEGORIES = ["Electronics", "Clothing", "Home", "Sports", "Books"]
REGIONS = ["North", "South", "East", "West"]

class StreamingEngine:
    def __init__(self):
        self.reset()

    def reset(self):
        self.hll = HLLCounter(precision=12)
        self.cms = CountMinSketch(width=5000, depth=5)
        self.total_count = 0
        self.running_sum = 0.0
        self.running_mean = 0.0
        self.transactions_per_second = 0
        self._second_counter = 0
        self._last_second = time.time()
        self.history = [] 

    def ingest(self, transaction: Dict[str, Any]):
        self.total_count += 1
        self._second_counter += 1
        self.hll.add(transaction["user_id"])
        self.cms.add(transaction["product_category"])
        amount = transaction["amount"]
        self.running_sum += amount
        self.running_mean += (amount - self.running_mean) / self.total_count

        now = time.time()
        if now - self._last_second >= 1.0:
            self.transactions_per_second = self._second_counter
            self._second_counter = 0
            self._last_second = now

    def get_snapshot(self) -> Dict[str, Any]:
        category_counts = {}
        for cat in CATEGORIES:
            category_counts[cat] = self.cms.estimate(cat)

        snapshot = {
            "timestamp": time.time(),
            "total_transactions": self.total_count,
            "unique_users": self.hll.estimate_cardinality(),
            "running_avg_amount": round(self.running_mean, 2),
            "running_total_amount": round(self.running_sum, 2),
            "transactions_per_second": self.transactions_per_second,
            "category_distribution": category_counts,
        }
        self.history.append(snapshot)
        if len(self.history) > 60:
            self.history = self.history[-60:]
        return snapshot

def generate_transaction(rng: np.random.Generator) -> Dict[str, Any]:
    # SCHEMA FIX: Removed 'quantity', matched names exactly to generate_data.py
    category_weights = [0.25, 0.25, 0.20, 0.15, 0.15]
    region_weights = [0.30, 0.30, 0.20, 0.20]

    return {
        "transaction_id": int(rng.integers(2000001, 9999999)),
        "user_id": int(rng.integers(1, 500000)),
        "product_category": rng.choice(CATEGORIES, p=category_weights),
        "amount": round(float(rng.uniform(5.0, 1500.0)), 2),
        "region": rng.choice(REGIONS, p=region_weights),
    }

async def stream_data(websocket, engine: StreamingEngine):
    rng = np.random.default_rng()
    batch_size = 50  
    try:
        while True:
            for _ in range(batch_size):
                txn = generate_transaction(rng)
                engine.ingest(txn)
            snapshot = engine.get_snapshot()
            await websocket.send_json(snapshot)
            await asyncio.sleep(0.5)
    except Exception as e:
        # DO NOT SILENTLY PASS! Print the exact error so we can debug it:
        import traceback
        traceback.print_exc()
        print(f"❌ WEBSOCKET CRASHED: {str(e)}")