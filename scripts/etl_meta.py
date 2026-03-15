import duckdb

def etl_ensure_meta(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS etl;")
    con.execute("CREATE SCHEMA IF NOT EXISTS bronze;")
    con.execute("CREATE SCHEMA IF NOT EXISTS silver;")
    con.execute("CREATE SCHEMA IF NOT EXISTS gold;")

    con.execute("""
        CREATE TABLE IF NOT EXISTS etl.job_state (
            job_id VARCHAR PRIMARY KEY,
            last_success_utc TIMESTAMP,
            last_report_date DATE,
            last_reference_code VARCHAR,
            last_rows BIGINT,
            last_error VARCHAR
        );
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS etl.job_runs (
            run_utc TIMESTAMP,
            phase VARCHAR,
            job_id VARCHAR,
            query_id VARCHAR,
            status VARCHAR,
            reference_code VARCHAR,
            report_date DATE,
            rows BIGINT,
            error VARCHAR
        );
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS etl.gold_assets (
            dataset_id VARCHAR PRIMARY KEY,
            path VARCHAR,
            kind VARCHAR,            -- file/folder
            partition_by VARCHAR,
            updated_utc TIMESTAMP,
            notes TEXT
    );
    """)
    