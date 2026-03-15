import os
import duckdb
import pandas as pd

def duck_write_parquet_atomic(con: duckdb.DuckDBPyConnection, df: pd.DataFrame, out_path: str) -> None:
    tmp_path = out_path + ".tmp"
    # register df as view, then COPY to parquet
    con.register("df_in", df)
    con.execute(f"""
        COPY (SELECT * FROM df_in)
        TO '{tmp_path.replace("'", "''")}'
        (FORMAT parquet, COMPRESSION zstd);
    """)
    os.replace(tmp_path, out_path)

def etl_ensure_meta(con):
    con.execute("CREATE SCHEMA IF NOT EXISTS etl;")
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")

    con.execute("""
        CREATE TABLE IF NOT EXISTS etl.job_state (
            job_id TEXT PRIMARY KEY,
            last_success_utc TIMESTAMP,
            last_report_date DATE,
            last_reference_code TEXT,
            last_rows BIGINT,
            last_error TEXT
        );
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS etl.job_runs (
            run_utc TIMESTAMP,
            job_id TEXT,
            query_id BIGINT,
            status TEXT,               -- ok / error / skipped
            reference_code TEXT,
            report_date DATE,
            rows BIGINT,
            error TEXT
        );
    """)
