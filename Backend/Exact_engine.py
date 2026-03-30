"""
Exact_engine.py
----------------
Executes EXACT analytical queries using DuckDB.
"""

import time
import duckdb
import pandas as pd
from typing import Optional, Dict, Any

class ExactEngine:
    def __init__(self, parquet_path: str):
        self.parquet_path = parquet_path
        self.conn = duckdb.connect(database=":memory:")

        # Load Parquet into a DuckDB table for fast SQL queries
        self.conn.execute(f"""
            CREATE TABLE transactions AS
            SELECT * FROM read_parquet('{parquet_path.replace(chr(92), '/')}')
        """)

        result = self.conn.execute("SELECT COUNT(*) FROM transactions").fetchone()
        self.total_rows = result[0]
        print(f"✅ ExactEngine (DuckDB) loaded Read/Write table from: {parquet_path}")

    def count(self, column: str = "*", where: Optional[str] = None) -> Dict[str, Any]:
        where_clause = f"WHERE {where}" if where else ""
        sql = f"SELECT COUNT({column}) FROM transactions {where_clause}"
        start = time.perf_counter()
        result = self.conn.execute(sql).fetchone()[0]
        elapsed = time.perf_counter() - start
        return {"query_type": "COUNT", "result": int(result), "time_ms": round(elapsed * 1000, 2), "engine": "exact"}

    def count_distinct(self, column: str, where: Optional[str] = None) -> Dict[str, Any]:
        where_clause = f"WHERE {where}" if where else ""
        sql = f"SELECT COUNT(DISTINCT {column}) FROM transactions {where_clause}"
        start = time.perf_counter()
        result = self.conn.execute(sql).fetchone()[0]
        elapsed = time.perf_counter() - start
        return {"query_type": "COUNT_DISTINCT", "result": int(result), "time_ms": round(elapsed * 1000, 2), "engine": "exact"}

    def sum(self, column: str, where: Optional[str] = None) -> Dict[str, Any]:
        where_clause = f"WHERE {where}" if where else ""
        sql = f"SELECT SUM({column}) FROM transactions {where_clause}"
        start = time.perf_counter()
        result = self.conn.execute(sql).fetchone()[0]
        elapsed = time.perf_counter() - start
        return {"query_type": "SUM", "result": round(float(result), 2) if result else 0, "time_ms": round(elapsed * 1000, 2), "engine": "exact"}

    def avg(self, column: str, where: Optional[str] = None) -> Dict[str, Any]:
        where_clause = f"WHERE {where}" if where else ""
        sql = f"SELECT AVG({column}) FROM transactions {where_clause}"
        start = time.perf_counter()
        result = self.conn.execute(sql).fetchone()[0]
        elapsed = time.perf_counter() - start
        return {"query_type": "AVG", "result": round(float(result), 2) if result else 0, "time_ms": round(elapsed * 1000, 2), "engine": "exact"}

    def group_by(self, group_column: str, agg_column: str, agg_func: str = "AVG", where: Optional[str] = None) -> Dict[str, Any]:
        where_clause = f"WHERE {where}" if where else ""
        sql = f"SELECT {group_column}, {agg_func}({agg_column}) as agg_value FROM transactions {where_clause} GROUP BY {group_column} ORDER BY {group_column}"
        start = time.perf_counter()
        rows = self.conn.execute(sql).fetchall()
        elapsed = time.perf_counter() - start
        result = {str(row[0]): round(float(row[1]), 2) for row in rows}
        return {"query_type": "GROUP_BY", "result": result, "time_ms": round(elapsed * 1000, 2), "engine": "exact"}

    def get_columns(self):
        info = self.conn.execute("DESCRIBE transactions").fetchall()
        return [{"name": row[0], "type": row[1]} for row in info]

    def get_sample_rows(self, n: int = 5):
        rows = self.conn.execute(f"SELECT * FROM transactions LIMIT {n}").fetchdf()
        return rows.to_dict(orient="records")

    def get_dataframe(self) -> pd.DataFrame:
        return self.conn.execute("SELECT * FROM transactions").fetchdf()