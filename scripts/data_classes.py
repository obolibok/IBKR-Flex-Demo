from __future__ import annotations

from dataclasses import dataclass
from typing import List
import datetime as dt


@dataclass
class IbkrCfg:
    token: str
    base_url: str
    version: int


@dataclass
class EtlCfg:
    poll_seconds: int
    max_wait_seconds: int
    min_seconds_between_runs: int
    pause_between_jobs_seconds: int
    initial_wait_seconds: int
    bronze: bool
    silver: bool
    gold: bool
    obfuscate: bool


@dataclass
class PathsCfg:
    kit_root: str
    duckdb_path: str
    gold_root: str


@dataclass
class FlexJobCfg:
    id: str
    enabled: bool
    query_id: str
    handler: str  # "module.path:ClassName"


@dataclass
class Config:
    ibkr: IbkrCfg
    etl: EtlCfg
    paths: PathsCfg
    flex_jobs: List[FlexJobCfg]


@dataclass
class PhaseLogRecord:
    run_utc: dt.datetime
    phase: str                  # bronze / silver / gold
    job_id: str
    query_id: str | None
    status: str                 # ok / skipped / error
    reason: str | None = None
    reference_code: str | None = None
    report_date: dt.date | None = None
    rows: int | None = None
