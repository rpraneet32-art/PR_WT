"""
main.py
--------
FastAPI application — the main entry point for the backend.
"""
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, UploadFile, File
import shutil
import os
import time
from typing import Optional
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# --- PATH FIXES: Importing directly from your backend/ folder ---
from Exact_engine import ExactEngine
from approx_engine import ApproxEngine
from streaming import StreamingEngine, stream_data

# ──────────────────── App Setup ────────────────────
app = FastAPI(
    title="Approximate Query Engine",
    description="High-Speed Analytical Insights with Approximate Query Processing",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ──────────────────── Data Loading ────────────────────
# PATH FIX: Pointing to backend/data/ecommerce.parquet
DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
PARQUET_PATH = os.path.join(DATA_DIR, "ecommerce.parquet")

# Initialize engines
exact_engine = ExactEngine(PARQUET_PATH)
df_full = exact_engine.get_dataframe()

# Streaming engine
streaming_engine = StreamingEngine()

# ──────────────────── Request Models ────────────────────
class QueryRequest(BaseModel):
    query_type: str  
    column: str = "*"  
    where: Optional[str] = None  
    group_by_column: Optional[str] = None  
    agg_func: Optional[str] = "AVG"  
    accuracy_target: float = 0.95  

class BenchmarkRequest(BaseModel):
    accuracy_levels: list = [0.80, 0.85, 0.90, 0.95, 0.99]
    query_types: list = ["count_distinct", "sum", "avg"]
    column: str = "amount"
    iterations: int = 3

# ──────────────────── Endpoints ────────────────────
@app.get("/")
def root():
    return {"status": "ok", "message": "Approximate Query Engine API"}

@app.get("/api/data/info")
def data_info():
    return {
        "total_rows": exact_engine.total_rows,
        "columns": exact_engine.get_columns(),
        "sample_rows": exact_engine.get_sample_rows(5),
    }

@app.post("/api/query/exact")
def run_exact_query(req: QueryRequest):
    return _dispatch_query(req, engine_type="exact")

@app.post("/api/query/approximate")
def run_approximate_query(req: QueryRequest):
    return _dispatch_query(req, engine_type="approximate")

@app.post("/api/query/compare")
def run_comparison_query(req: QueryRequest):
    exact_result = _dispatch_query(req, engine_type="exact")
    approx_result = _dispatch_query(req, engine_type="approximate")

    exact_val = exact_result.get("result", 0)
    approx_val = approx_result.get("result", 0)

    if isinstance(exact_val, dict) and isinstance(approx_val, dict):
        errors = []
        for key in exact_val:
            if key in approx_val and exact_val[key] != 0:
                error = abs(exact_val[key] - approx_val[key]) / abs(exact_val[key]) * 100
                errors.append(error)
        error_pct = round(sum(errors) / len(errors), 2) if errors else 0
    elif exact_val != 0:
        error_pct = round(abs(exact_val - approx_val) / abs(exact_val) * 100, 2)
    else:
        error_pct = 0

    exact_time = exact_result.get("time_ms", 1)
    approx_time = approx_result.get("time_ms", 1)
    speedup = round(exact_time / max(approx_time, 0.01), 2)

    return {
        "exact": exact_result,
        "approximate": approx_result,
        "comparison": {
            "error_pct": error_pct,
            "accuracy_pct": round(100 - error_pct, 2),
            "speedup": speedup,
            "exact_time_ms": exact_time,
            "approx_time_ms": approx_time,
            "exact_memory_bytes": exact_result.get("memory_bytes", 1),
            "approx_memory_bytes": approx_result.get("memory_bytes", 1),
        },
    }

@app.post("/api/benchmark")
def run_benchmark(req: BenchmarkRequest):
    results = []
    for accuracy in req.accuracy_levels:
        for query_type in req.query_types:
            times_exact = []
            times_approx = []
            errors = []

            for _ in range(req.iterations):
                query_req = QueryRequest(
                    query_type=query_type,
                    column=req.column,
                    accuracy_target=accuracy,
                )
                comparison = run_comparison_query(query_req)
                times_exact.append(comparison["exact"]["time_ms"])
                times_approx.append(comparison["approximate"]["time_ms"])
                errors.append(comparison["comparison"]["error_pct"])

            results.append({
                "accuracy_target": accuracy,
                "query_type": query_type,
                "avg_exact_time_ms": round(sum(times_exact) / len(times_exact), 2),
                "avg_approx_time_ms": round(sum(times_approx) / len(times_approx), 2),
                "avg_error_pct": round(sum(errors) / len(errors), 2),
                "avg_speedup": round((sum(times_exact) / len(times_exact)) / max(sum(times_approx) / len(times_approx), 0.01), 2),
            })
    return {"benchmarks": results}

@app.websocket("/ws/stream")
async def websocket_stream(websocket: WebSocket):
    await websocket.accept()
    engine = StreamingEngine()
    try:
        await stream_data(websocket, engine)
    except WebSocketDisconnect:
        pass

# ──────────────────── Internal Helpers ────────────────────
def _dispatch_query(req: QueryRequest, engine_type: str) -> dict:
    query_type = req.query_type.lower().strip()

    if engine_type == "exact":
        engine = exact_engine
        if query_type == "count":
            return engine.count(req.column, req.where)
        elif query_type == "count_distinct":
            return engine.count_distinct(req.column, req.where)
        elif query_type == "sum":
            return engine.sum(req.column, req.where)
        elif query_type == "avg":
            return engine.avg(req.column, req.where)
        elif query_type == "group_by":
            return engine.group_by(req.group_by_column or "product_category", req.column, req.agg_func or "AVG", req.where)
    else:
        engine = ApproxEngine(df_full, accuracy_target=req.accuracy_target)
        if query_type == "count":
            return engine.count(req.column, req.where)
        elif query_type == "count_distinct":
            return engine.count_distinct(req.column, req.where)
        elif query_type == "sum":
            return engine.sum(req.column, req.where)
        elif query_type == "avg":
            return engine.avg(req.column, req.where)
        elif query_type == "group_by":
            return engine.group_by(req.group_by_column or "product_category", req.column, req.agg_func or "AVG", req.where)

    return {"error": f"Unknown query type: {query_type}"}
@app.post("/api/upload")
async def upload_custom_dataset(file: UploadFile = File(...)):
    """Allows users to upload their own CSV or Parquet files for real-world testing!"""
    global exact_engine, df_full
    
    try:
        # 1. Save the uploaded file temporarily
        file_ext = file.filename.split('.')[-1].lower()
        temp_path = os.path.join(DATA_DIR, f"uploaded_temp.{file_ext}")
        
        with open(temp_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
            
        # 2. Convert to Parquet if it's a CSV (DuckDB and Pandas prefer Parquet)
        final_path = os.path.join(DATA_DIR, "custom_dataset.parquet")
        if file_ext == "csv":
            print("⏳ Converting uploaded CSV to Parquet for performance...")
            import pandas as pd
            temp_df = pd.read_csv(temp_path)
            temp_df.to_parquet(final_path)
            os.remove(temp_path)
        else:
            # If it's already a parquet, just rename it
            os.rename(temp_path, final_path)

        # 3. CLEAR THE APPROX ENGINE CACHE (Crucial step!)
        from approx_engine import clear_cache
        clear_cache()

        # 4. Hot-Swap the Exact Engine
        print("🔄 Hot-swapping to new custom dataset...")
        exact_engine = ExactEngine(final_path)
        df_full = exact_engine.get_dataframe()
        
        return {
            "status": "success",
            "message": f"Successfully loaded {file.filename}!",
            "new_total_rows": exact_engine.total_rows,
            "columns": exact_engine.get_columns()
        }
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"error": f"Failed to process file: {str(e)}"}