from __future__ import annotations

from typing import Dict
import os
import yaml

from scripts.data_classes import Config, IbkrCfg, EtlCfg, PathsCfg, FlexJobCfg


def _resolve_path(template: str, kit_root: str) -> str:
    return str(template).replace("{kit_root}", kit_root)


def cfg_load_config(config_path: str) -> Config:
    with open(config_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)

    ib = raw["ibkr"]
    etl = raw.get("etl", {})
    paths = raw.get("paths", {})

    kit_root = str(paths["kit_root"]).rstrip("\\/")

    duckdb_path = _resolve_path(paths.get("duckdb_path", r"{kit_root}\cache\warehouse.duckdb"), kit_root)
    gold_root = _resolve_path(paths.get("gold_root", r"{kit_root}\cache\mart"), kit_root)

    # flex_jobs: новый формат
    jobs_raw = raw.get("flex_jobs")

    flex_jobs = [
        FlexJobCfg(
            id=str(j["id"]),
            enabled=bool(j.get("enabled", True)),
            query_id=str(j["query_id"]),
            handler=str(j["handler"]),
        )
        for j in jobs_raw
    ]

    return Config(
        ibkr=IbkrCfg(
            token=str(ib["token"]),
            base_url=str(ib["base_url"]).rstrip("/"),
            version=int(ib.get("version", 3)),
        ),
        etl=EtlCfg(
            poll_seconds=int(etl.get("poll_seconds", 10)),
            max_wait_seconds=int(etl.get("max_wait_seconds", 300)),
            min_seconds_between_runs=int(etl.get("min_seconds_between_runs", 120)),
            bronze=bool(etl.get("bronze", True)),
            silver=bool(etl.get("silver", True)),
            gold=bool(etl.get("gold", True)),
            obfuscate=bool(etl.get("obfuscate", False)),
        ),
        paths=PathsCfg(
            kit_root=kit_root,
            duckdb_path=duckdb_path,
            gold_root=gold_root,
        ),
        flex_jobs=flex_jobs,
    )


def cfg_ensure_dirs(cfg: Config) -> Dict[str, str]:
    cache_root = os.path.join(cfg.paths.kit_root, "cache")
    os.makedirs(cache_root, exist_ok=True)

    os.makedirs(os.path.dirname(cfg.paths.duckdb_path), exist_ok=True)
    os.makedirs(cfg.paths.gold_root, exist_ok=True)

    return {
        "cache_root": cache_root,
        "duckdb_path": cfg.paths.duckdb_path,
        "gold_root": cfg.paths.gold_root,
    }