from __future__ import annotations

from dataclasses import dataclass
import datetime as dt
import duckdb
import os

from scripts.data_classes import Config, FlexJobCfg
from scripts.etl_manifest import register_asset


@dataclass
class JobContext:
    cfg: Config
    con: duckdb.DuckDBPyConnection


@dataclass
class JobResult:
    job_id: str
    query_id: str | None
    status: str            # ok / skipped / error
    reason: str
    reference_code: str | None
    report_date: dt.date | None
    rows: int


class FlexJob:
    def __init__(self, job: FlexJobCfg, cfg: Config):
        self.job = job
        self.cfg = cfg

    def ensure_storage(self, ctx: JobContext) -> None:
        con = ctx.con
        con.execute("""CREATE TABLE IF NOT EXISTS silver.symbols(
            map_to VARCHAR,
            symbol VARCHAR,
            conid BIGINT,
            description VARCHAR,
            first_seen DATE,
            src VARCHAR);""")

    def run_bronze(self, ctx: JobContext) -> JobResult:
        raise NotImplementedError

    def update_silver(self, ctx: JobContext) -> None:
        con = ctx.con

        # try to set map to earlier symbol by conid
        con.execute("""
            UPDATE silver.symbols AS sy
            SET map_to = x.symbol
            FROM (
                SELECT
                    sy2.symbol AS sy_symbol,
                    sy2.conid  AS sy_conid,
                    src.symbol,
                    ROW_NUMBER() OVER (
                        PARTITION BY sy2.conid, sy2.symbol
                        ORDER BY src.first_seen
                    ) AS rn
                FROM silver.symbols AS sy2
                JOIN silver.symbols AS src
                  ON sy2.conid = src.conid
                 AND sy2.first_seen > src.first_seen
            ) AS x
            WHERE sy.symbol = x.sy_symbol
              AND sy.conid = x.sy_conid
              AND x.rn = 1
              AND sy.map_to IS NULL;
        """)

        # try to set map to earlier symbol by company name
        con.execute("""
            UPDATE silver.symbols AS sy
            SET map_to = x.symbol
            FROM (
                SELECT
                    sy2.symbol AS sy_symbol,
                    sy2.description AS sy_description,
                    src.symbol,
                    ROW_NUMBER() OVER (
                        PARTITION BY sy2.description, sy2.symbol
                        ORDER BY src.first_seen
                    ) AS rn
                FROM silver.symbols AS sy2
                JOIN silver.symbols AS src
                  ON sy2.description = src.description
                 AND sy2.first_seen > src.first_seen
            ) AS x
            WHERE sy.symbol = x.sy_symbol
              AND sy.description = x.sy_description
              AND x.rn = 1
              AND sy.map_to IS NULL;
        """)

        # map rest to itself
        con.execute("""
            UPDATE silver.symbols
            SET map_to = symbol
            WHERE map_to IS NULL;
        """)

    def build_gold(self, ctx: JobContext) -> None:
        con = ctx.con
        gold_dir = os.path.join(ctx.cfg.paths.gold_root, "symbols")
        os.makedirs(gold_dir, exist_ok=True)

        history_path = os.path.join(gold_dir, "dictionary.parquet")
        history_sql = history_path.replace("\\", "/").replace("'", "''")

        con.execute(f"""
            COPY (
                SELECT DISTINCT x.map_to AS symbol, x.description
                FROM (
                    SELECT
                        sy2.map_to AS map_to,
                        src.description,
                        ROW_NUMBER() OVER (
                            PARTITION BY sy2.map_to
                            ORDER BY src.first_seen DESC
                        ) AS rn
                    FROM silver.symbols AS sy2
                    JOIN silver.symbols AS src
                      ON sy2.map_to = src.map_to
                     AND sy2.first_seen <= src.first_seen
                     AND src.src IN ('trades', 'positions')
                ) AS x,
                silver.symbols AS sy
                WHERE sy.map_to = x.map_to
                  AND x.rn = 1
            )
            TO '{history_sql}'
            (FORMAT parquet, COMPRESSION zstd);
        """)

        register_asset(con, "symbols_dictionary", history_path, "file", None, "Symbols dictionary (single parquet)")
