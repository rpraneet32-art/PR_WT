import time
import random
import sys
import os

# Add the parent directory to the path so we can import your engine
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from Exact_engine import ExactEngine

def run_automated_suite():
    print("Starting Automated Benchmark Suite (Baseline)...")
    
    # Initialize your engine
    engine = ExactEngine()
    
    query_types = ["COUNT", "SUM", "AVG"]
    columns = ["amount"]
    
    results_log = []
    
    print("\nRunning 50 Baseline Queries...")
    
    # Run 50 automated queries to get the average EXACT execution time
    for i in range(50):
        q_type = random.choice(query_types)
        col = random.choice(columns)
        
        # We are using your exact_engine here!
        _, exec_time = engine.run_query(q_type, col)
        
        results_log.append({
            "run": i + 1,
            "query": f"{q_type}({col})",
            "time_ms": exec_time
        })
        
        # Print a neat progress bar
        sys.stdout.write(f"\rProgress: [{'=' * (i // 2)}{' ' * (25 - (i // 2))}] {i + 1}/50")
        sys.stdout.flush()

    print("\n\nBenchmark Suite Complete!\n")
    
    # Calculate Averages
    avg_count = sum(r["time_ms"] for r in results_log if "COUNT" in r["query"]) / max(1, len([r for r in results_log if "COUNT" in r["query"]]))
    avg_sum = sum(r["time_ms"] for r in results_log if "SUM" in r["query"]) / max(1, len([r for r in results_log if "SUM" in r["query"]]))
    avg_avg = sum(r["time_ms"] for r in results_log if "AVG" in r["query"]) / max(1, len([r for r in results_log if "AVG" in r["query"]]))

    # Print the final report for the judges' documentation
    print("=========================================")
    print("  BASELINE PERFORMANCE REPORT (Exact) ")
    print("=========================================")
    print(f"Dataset Size: ~2,000,000 rows")
    print(f"Average time for COUNT: {avg_count:.2f} ms")
    print(f"Average time for SUM:   {avg_sum:.2f} ms")
    print(f"Average time for AVG:   {avg_avg:.2f} ms")
    print("=========================================")
    print("Target Approximate Time to hit 3x Speedup:")
    print(f"--> COUNT needs to be under: {(avg_count / 3):.2f} ms")
    print(f"--> SUM needs to be under:   {(avg_sum / 3):.2f} ms")
    print("=========================================")

if __name__ == "__main__":
    run_automated_suite()