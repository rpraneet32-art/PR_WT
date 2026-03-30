import time
import random
import sys
import os

# Add the parent directory to the path so we can import your engines
backend_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(backend_dir)

from Exact_engine import ExactEngine
from approx_engine import ApproxEngine

def run_automated_suite():
    print("Starting Automated Benchmark Suite (Side-by-Side Comparison)...")
    
    # 1. Properly resolve data path and initialize BOTH engines
    parquet_path = os.path.join(backend_dir, "data", "ecommerce.parquet")
    
    print("\nBooting Exact Engine (DuckDB)...")
    exact_engine = ExactEngine(parquet_path)
    
    print("Booting Approx Engine (RAM Synopsis)...")
    df_full = exact_engine.get_dataframe()
    approx_engine = ApproxEngine(df_full, accuracy_target=0.95)
    
    query_types = ["COUNT", "SUM", "AVG"]
    columns = ["amount"]
    
    results_log = []
    
    print("\nRunning 50 Automated Comparison Queries...")
    
    # Run 50 automated queries on both engines simultaneously
    for i in range(50):
        q_type = random.choice(query_types)
        col = random.choice(columns)
        
        # Run EXACT
        e_val, e_time = exact_engine.count(col) if q_type == "COUNT" else \
                        exact_engine.sum(col) if q_type == "SUM" else \
                        exact_engine.avg(col)
                        
        # Run APPROX
        a_val, a_time = approx_engine.count(col) if q_type == "COUNT" else \
                        approx_engine.sum(col) if q_type == "SUM" else \
                        approx_engine.avg(col)
        
        # Extract the actual time from the returned dictionaries
        e_time_ms = e_time["time_ms"] if isinstance(e_time, dict) else e_time
        a_time_ms = a_time["time_ms"] if isinstance(a_time, dict) else a_time
        
        results_log.append({
            "run": i + 1,
            "query": f"{q_type}({col})",
            "exact_time_ms": e_time_ms,
            "approx_time_ms": a_time_ms,
            "speedup": e_time_ms / max(a_time_ms, 0.01)
        })
        
        # Print a neat progress bar
        sys.stdout.write(f"\rProgress: [{'=' * (i // 2)}{' ' * (25 - (i // 2))}] {i + 1}/50")
        sys.stdout.flush()

    print("\n\nBenchmark Suite Complete!\n")
    
    # Calculate Averages
    def get_avg(q_type, metric):
        items = [r[metric] for r in results_log if q_type in r["query"]]
        return sum(items) / max(1, len(items))

    print("==================================================")
    print("      FINAL PERFORMANCE REPORT (50 QUERIES)       ")
    print("==================================================")
    print(f"Dataset Size: ~{exact_engine.total_rows:,} rows")
    print("==================================================")
    
    for qt in ["COUNT", "SUM", "AVG"]:
        e_avg = get_avg(qt, "exact_time_ms")
        a_avg = get_avg(qt, "approx_time_ms")
        spd_avg = get_avg(qt, "speedup")
        
        print(f"[{qt}] Exact (DuckDB):   {e_avg:.2f} ms")
        print(f"[{qt}] Approx (Synopsis): {a_avg:.2f} ms")
        print(f"      --> SPEEDUP:        {spd_avg:.2f}x Faster")
        print("--------------------------------------------------")
        
    print("Hackathon Target: 3.00x Speedup")
    print("==================================================")

if __name__ == "__main__":
    run_automated_suite()