import os
import datetime as dt
import duckdb

def register_asset(con: duckdb.DuckDBPyConnection, dataset_id: str, path: str, kind: str, partition_by: str | None, notes: str = ""):
    con.execute("""
        INSERT INTO etl.gold_assets(dataset_id, path, kind, partition_by, updated_utc, notes)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(dataset_id) DO UPDATE SET
          path=excluded.path,
          kind=excluded.kind,
          partition_by=excluded.partition_by,
          updated_utc=excluded.updated_utc,
          notes=excluded.notes
    """, [dataset_id, path, kind, partition_by, dt.datetime.utcnow(), notes])

def write_manifest_parquet(con: duckdb.DuckDBPyConnection, gold_root: str):
    out_path = os.path.join(gold_root, "_manifest", "gold_assets.parquet")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    con.execute(f"""
        COPY (SELECT * FROM etl.gold_assets ORDER BY dataset_id)
        TO '{out_path.replace("\\\\","/").replace("'", "''")}'
        (FORMAT parquet, COMPRESSION zstd);
    """)
    return out_path