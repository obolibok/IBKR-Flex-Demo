from __future__ import annotations

import random
import string
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable

import duckdb


_ALPHABET = string.ascii_uppercase


@dataclass
class ObfuscationContext:
    factor: float
    symbol_map: dict[str, str]


_MONEY_LIKE_COLUMNS = {
    "tradeprice",
    "trademoney",
    "proceeds",
    "taxes",
    "ibcommission",
    "netcash",
    "closeprice",
    "cost",
    "costbasis",
    "fifopnlrealized",
    "mtmpnl",
    "marketvalue",
    "positionvalue",
    "costbasismoney",
    "fifopnlunrealized",
    "cash",
    "stock",
    "options",
    "funds",
    "dividendaccruals",
    "interestaccruals",
    "forexcfdunrealizedpl",
    "cfdunrealizedpl",
    "crypto",
    "total",
    "totallong",
    "totalshort",
    "amount",
    "amountbase",
    "price",
    "avgprice",
    "averageprice",
    "markprice",
    "strike",
    "value",
}

# Обфусцируем только эти витрины.
_OBFUSCATE_DATASET_IDS = {
    "trades_history",
    "positions_latest",
    "positions_history",
    "cash_transactions_history",
    "account_nav_latest",
    "account_nav_history",
    "symbols_dictionary",
    "corporate_actions_history",
}


def _should_obfuscate_dataset(dataset_id: str) -> bool:
    return dataset_id in _OBFUSCATE_DATASET_IDS


def _num_to_code(n: int, width: int = 4) -> str:
    base = len(_ALPHABET)
    chars: list[str] = []

    for _ in range(width):
        chars.append(_ALPHABET[n % base])
        n //= base

    return "".join(reversed(chars))


def _generate_factor(min_value: float = 2.0, max_value: float = 20.0, step: float = 0.1) -> float:
    min_i = int(round(min_value / step))
    max_i = int(round(max_value / step))
    return random.randint(min_i, max_i) * step


def _load_gold_assets(con: duckdb.DuckDBPyConnection) -> list[tuple[str, str]]:
    rows = con.execute("""
        SELECT dataset_id, path
        FROM etl.gold_assets
        WHERE kind = 'file'
          AND lower(path) LIKE '%.parquet'
        ORDER BY dataset_id
    """).fetchall()

    return [(r[0], r[1]) for r in rows]


def _describe_columns(con: duckdb.DuckDBPyConnection, parquet_path: str) -> list[str]:
    path_sql = parquet_path.replace("\\", "/").replace("'", "''")
    rows = con.execute(f"""
        DESCRIBE SELECT * FROM read_parquet('{path_sql}')
    """).fetchall()

    return [row[0] for row in rows]


def _is_symbol_column(col_name: str) -> bool:
    c = col_name.lower()
    return c == "symbol" or c.startswith("symbol_")


def _collect_symbols_from_gold(con: duckdb.DuckDBPyConnection, assets: list[tuple[str, str]]) -> list[str]:
    symbols: set[str] = set()

    for dataset_id, path in assets:
        if not _should_obfuscate_dataset(dataset_id):
            continue

        if not Path(path).exists():
            continue

        path_sql = path.replace("\\", "/").replace("'", "''")
        columns = _describe_columns(con, path)
        symbol_columns = [c for c in columns if _is_symbol_column(c)]

        if not symbol_columns:
            continue

        for symbol_col in symbol_columns:
            quoted = f'"{symbol_col}"'
            rows = con.execute(f"""
                SELECT DISTINCT CAST({quoted} AS VARCHAR) AS symbol
                FROM read_parquet('{path_sql}')
                WHERE {quoted} IS NOT NULL
                  AND trim(CAST({quoted} AS VARCHAR)) <> ''
            """).fetchall()

            for row in rows:
                symbols.add(row[0])

    return sorted(symbols)


def _build_symbol_map(symbols: Iterable[str]) -> dict[str, str]:
    uniq = sorted(set(symbols))
    return {symbol: _num_to_code(i) for i, symbol in enumerate(uniq)}


def create_obfuscation_context(con: duckdb.DuckDBPyConnection) -> ObfuscationContext:
    assets = _load_gold_assets(con)
    symbols = _collect_symbols_from_gold(con, assets)

    return ObfuscationContext(
        factor=_generate_factor(2.0, 20.0, 0.1),
        symbol_map=_build_symbol_map(symbols),
    )


def _ensure_temp_symbol_map(con: duckdb.DuckDBPyConnection, symbol_map: dict[str, str]) -> None:
    con.execute("DROP TABLE IF EXISTS temp_obfuscation_symbol_map;")
    con.execute("""
        CREATE TEMP TABLE temp_obfuscation_symbol_map(
            symbol VARCHAR,
            obfuscated_symbol VARCHAR
        );
    """)

    rows = [(k, v) for k, v in symbol_map.items()]
    if rows:
        con.executemany("""
            INSERT INTO temp_obfuscation_symbol_map(symbol, obfuscated_symbol)
            VALUES (?, ?)
        """, rows)


def _symbol_expr(col_name: str) -> str:
    quoted = f'"{col_name}"'
    return f'COALESCE(m_{col_name}.obfuscated_symbol, CAST(src.{quoted} AS VARCHAR)) AS "{col_name}"'


def _money_expr(col_name: str, factor: float) -> str:
    quoted = f'"{col_name}"'
    return f'CASE WHEN src.{quoted} IS NULL THEN NULL ELSE src.{quoted} * {factor} END AS "{col_name}"'


def _default_expr(col_name: str) -> str:
    quoted = f'"{col_name}"'
    return f'src.{quoted} AS "{col_name}"'


def _company_description_expr(symbol_col: str = "symbol") -> str:
    alias = f"m_{symbol_col}"
    return (
        f"CASE "
        f"WHEN {alias}.obfuscated_symbol IS NOT NULL THEN 'Company ' || {alias}.obfuscated_symbol "
        f"ELSE src.\"description\" "
        f"END AS \"description\""
    )


def _cash_description_expr() -> str:
    return r'''
        CASE
            WHEN src."description" IS NULL THEN NULL
            WHEN m_symbol.obfuscated_symbol IS NULL THEN src."description"
            WHEN regexp_matches(src."description", '^[A-Za-z0-9._-]+\([^)]+\)\s+.*')
                THEN m_symbol.obfuscated_symbol || ' ' ||
                     regexp_replace(
                         src."description",
                         '^[A-Za-z0-9._-]+\([^)]+\)\s+',
                         ''
                     )
            ELSE src."description"
        END AS "description"
    '''


def _corporate_action_description_expr() -> str:
    return """
        CASE
            WHEN m_symbol.obfuscated_symbol IS NOT NULL
                THEN 'Corporate action for ' || m_symbol.obfuscated_symbol
            ELSE 'Corporate action'
        END AS "description"
    """


def _corporate_action_action_description_expr() -> str:
    return """
        CASE
            WHEN src."type" IS NOT NULL AND src."code" IS NOT NULL
                THEN src."type" || ' / ' || src."code"
            WHEN src."type" IS NOT NULL
                THEN src."type"
            WHEN src."code" IS NOT NULL
                THEN src."code"
            ELSE 'Corporate action'
        END AS "actionDescription"
    """


def _make_select_sql(
    con: duckdb.DuckDBPyConnection,
    dataset_id: str,
    parquet_path: str,
    factor: float,
) -> str:
    path_sql = parquet_path.replace("\\", "/").replace("'", "''")
    columns = _describe_columns(con, parquet_path)

    select_parts: list[str] = []
    join_clauses: list[str] = []

    for col in columns:
        if _is_symbol_column(col):
            alias = f"m_{col}"
            quoted = f'"{col}"'
            join_clauses.append(
                f'LEFT JOIN temp_obfuscation_symbol_map AS {alias} '
                f'ON CAST(src.{quoted} AS VARCHAR) = {alias}.symbol'
            )

    for col in columns:
        col_l = col.lower()

        if _is_symbol_column(col):
            select_parts.append(_symbol_expr(col))
        elif col_l == "description" and dataset_id == "cash_transactions_history":
            select_parts.append(_cash_description_expr())
        elif col_l == "description" and dataset_id in {"symbols_dictionary", "positions_latest", "positions_history"}:
            select_parts.append(_company_description_expr("symbol"))
        elif col_l == "description" and dataset_id == "corporate_actions_history":
            select_parts.append(_corporate_action_description_expr())
        elif col_l == "actiondescription" and dataset_id == "corporate_actions_history":
            select_parts.append(_corporate_action_action_description_expr())
        elif col_l in _MONEY_LIKE_COLUMNS:
            select_parts.append(_money_expr(col, factor))
        else:
            select_parts.append(_default_expr(col))

    join_sql = "\n        ".join(join_clauses)

    return f"""
        SELECT
            {", ".join(select_parts)}
        FROM read_parquet('{path_sql}') AS src
        {join_sql}
    """


def _rewrite_parquet_in_place(
    con: duckdb.DuckDBPyConnection,
    parquet_path: str,
    select_sql: str,
) -> None:
    parquet = Path(parquet_path)
    tmp_path = parquet.with_suffix(".tmp.parquet")

    tmp_sql = str(tmp_path).replace("\\", "/").replace("'", "''")

    con.execute(f"""
        COPY (
            {select_sql}
        )
        TO '{tmp_sql}'
        (FORMAT parquet, COMPRESSION zstd);
    """)

    parquet.unlink(missing_ok=True)
    tmp_path.rename(parquet)


def obfuscate_gold_outputs(con: duckdb.DuckDBPyConnection) -> ObfuscationContext:
    assets = _load_gold_assets(con)
    ctx = create_obfuscation_context(con)

    _ensure_temp_symbol_map(con, ctx.symbol_map)

    for dataset_id, path in assets:
        if not _should_obfuscate_dataset(dataset_id):
            continue

        if not Path(path).exists():
            continue

        select_sql = _make_select_sql(con, dataset_id, path, ctx.factor)
        _rewrite_parquet_in_place(con, path, select_sql)

    return ctx
