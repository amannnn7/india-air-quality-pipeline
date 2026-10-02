"""One small interface for reading query results, whichever warehouse we are on.

The dashboard code calls `query(sql)` and gets back a list of dicts with lowercase keys.
It does not care whether the rows came from DuckDB or Snowflake.
"""

from __future__ import annotations

from pathlib import Path

from .settings import SnowflakeConfig


class Warehouse:
    def __init__(self, target: str, duckdb_path: Path | None = None):
        self.target = target
        if target == "snowflake":
            from .snowflake_load import connect

            cfg = SnowflakeConfig.from_env()
            self._conn = connect(cfg)
            self._conn.cursor().execute("use schema ANALYTICS")
        else:
            import duckdb

            # Not read_only: dbt may still hold a connection to this file in the same process,
            # and DuckDB only allows one configuration per file per process.
            self._conn = duckdb.connect(str(duckdb_path))

    @property
    def label(self) -> str:
        return "Snowflake" if self.target == "snowflake" else "DuckDB (local)"

    def query(self, sql: str) -> list[dict]:
        cur = self._conn.cursor()
        cur.execute(sql)
        cols = [c[0].lower() for c in cur.description]
        return [dict(zip(cols, row, strict=True)) for row in cur.fetchall()]

    def close(self) -> None:
        self._conn.close()
