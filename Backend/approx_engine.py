import pandas as pd
import time
import os

class ApproxEngine:
    def __init__(self, db_path="data/ecommerce.parquet"):
        current_dir = os.path.dirname(os.path.abspath(__file__))
        full_path = os.path.join(current_dir, "data", "ecommerce.parquet")
        
        try:
            print("🏗️ Loading Pre-Computed RAM Synopsis...")
            # 1. Load the raw data ONCE
            full_df = pd.read_parquet(full_path)
            self.total_rows = len(full_df)
            
            # 2. THE ENTERPRISE AQP SECRET: 
            # Pre-shuffle and hold exactly 10% of the data in pure RAM permanently.
            self.synopsis = full_df.sample(frac=0.10, random_state=42).reset_index(drop=True)
            print(f"⚡ RAM Synopsis Ready! Holding {len(self.synopsis)} rows in pure memory without SQL overhead.")
            
        except Exception as e:
            print(f"⚠️ Warning: ApproxEngine could not load dataset. Error: {e}")

    def run_approx_query(self, query_type, column, group_by=None, accuracy=0.95):
        # 1. Map accuracy to sample size
        base_fraction = (accuracy - 0.8) / 2  
        sample_percent = max(1, min(int(base_fraction * 100), 10)) 
        
        # Calculate exactly how many rows we need
        rows_to_read = int(self.total_rows * (sample_percent / 100))

        # 2. START THE CLOCK
        start_time = time.perf_counter()
        
        # 3. INSTANT SLICE: Grab the top N rows from RAM (O(1) time complexity, no random generation needed)
        active_sample = self.synopsis.head(rows_to_read)
        
        # 4. PURE VECTORIZED MATH (Bypassing SQL parsing)
        q_type = query_type.upper()
        
        # Scale sums and counts, but averages stay the same
        scale_factor = self.total_rows / rows_to_read if q_type in ["SUM", "COUNT"] else 1.0

        if group_by:
            if q_type == "COUNT":
                raw_res = active_sample.groupby(group_by)[column].count()
            elif q_type == "SUM":
                raw_res = active_sample.groupby(group_by)[column].sum()
            elif q_type == "AVG":
                raw_res = active_sample.groupby(group_by)[column].mean()
            
            # Scale group by dict
            result_value = (raw_res * scale_factor).to_dict()
        else:
            if q_type == "COUNT":
                raw_res = active_sample[column].count()
            elif q_type == "SUM":
                raw_res = active_sample[column].sum()
            elif q_type == "AVG":
                raw_res = active_sample[column].mean()
                
            result_value = float(raw_res * scale_factor)

        # 5. STOP THE CLOCK
        end_time = time.perf_counter()
        execution_time_ms = (end_time - start_time) * 1000  

        return result_value, execution_time_ms, sample_percent