from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional

# Import your engines
from Exact_engine import ExactEngine
from approx_engine import ApproxEngine
from benchmark.benchmark import calculate_metrics

app = FastAPI(title="WT'26 AQP Engine API")

# --- CORS SETUP ---
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- STARTUP INITIALIZATION ---
print("Initializing Exact Engine (DuckDB)...")
exact_engine = ExactEngine(db_path="data/ecommerce.parquet")

print("Initializing Approx Engine (DuckDB Native)...")
approx_engine = ApproxEngine(db_path="data/ecommerce.parquet")

# --- REQUEST MODEL ---
class QueryRequest(BaseModel):
    query_type: str  
    column: str      
    group_by: Optional[str] = None
    accuracy_target: Optional[float] = 0.95  

# --- ENDPOINTS ---
@app.get("/")
def read_root():
    return {"status": "AQP Engine is running!"}

@app.post("/api/benchmark")
def run_full_benchmark(request: QueryRequest):
    try:
        # 1. EXACT ENGINE
        e_val, exact_time = exact_engine.run_query(
            request.query_type, request.column, request.group_by
        )
        e_val_clean = e_val[0][0] if isinstance(e_val, list) else e_val

        # 2. APPROX ENGINE (DuckDB Native)
        a_val, approx_time_ms, sample_percent = approx_engine.run_approx_query(
            request.query_type, request.column, request.group_by, accuracy=request.accuracy_target
        )
        
        # 3. CALCULATE METRICS
        metrics = calculate_metrics(e_val_clean, a_val, exact_time, approx_time_ms)
        
        # Add the sample size used (estimated rows based on percentage)
        metrics["sample_size_used"] = int(2000000 * (sample_percent / 100))
        
        return metrics
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        raise HTTPException(status_code=400, detail=str(e))