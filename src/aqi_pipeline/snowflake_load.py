"""LOAD INTO SNOWFLAKE: push the landed Parquet files into the RAW layer of Snowflake.

This is the standard Snowflake loading pattern, step by step:

    1. PUT      local Parquet files  ->  an internal STAGE (Snowflake-managed file storage)
    2. COPY INTO  a temporary table  <-  the staged files (Snowflake reads Parquet natively)
    3. MERGE    the temporary table  ->  RAW.AIR_QUALITY_HOURLY  (update existing rows, insert new)

Why MERGE and not a plain INSERT? Because the daily job re-loads the last 3 days to catch
corrections. MERGE on the natural key (city_id, observed_at) makes that an *upsert*:
running the load twice leaves exactly one row per city-hour (idempotent).

In a bigger company the stage would usually be an EXTERNAL stage on S3/ADLS, and COPY
could be automated with Snowpipe. Only step 1 would change.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Sequence
from datetime import date
from pathlib import Path

from .settings import SnowflakeConfig

log = logging.getLogger(__name__)

RAW_TABLE = "AIR_QUALITY_HOURLY"
STAGE = "AQI_LANDING"
COLUMNS = ["CITY_ID", "OBSERVED_AT", "PM2_5", "PM10", "NO2", "SO2", "O3", "CO", "SOURCE", "INGESTED_AT", "RUN_ID"]
KEY = ["CITY_ID", "OBSERVED_AT"]


def ddl_statements(schema: str) -> list[str]:
    """Objects the loader needs. `if not exists` makes this safe to run on every load."""
    return [
        f"create schema if not exists {schema}",
        f"""create table if not exists {schema}.{RAW_TABLE} (
                city_id      varchar       not null,
                observed_at  timestamp_ntz not null  comment 'local India time (Asia/Kolkata)',
                pm2_5        float,
                pm10         float,
                no2          float,
                so2          float,
                o3           float,
                co           float                   comment 'ug/m3 as received; converted to mg/m3 in dbt',
                source       varchar,
                ingested_at  timestamp_tz,
                run_id       varchar,
                constraint pk_air_quality_hourly primary key (city_id, observed_at)
            ) comment = 'Hourly pollutant readings from Open-Meteo (CAMS). Loaded by aqi-pipeline.'""",
        f"create stage if not exists {schema}.{STAGE} file_format = (type = parquet)"
        " comment = 'Landing zone for Parquet files uploaded by aqi-pipeline'",
    ]


def put_statement(schema: str, partition_dir: Path) -> str:
    """Upload every Parquet file of one date partition, keeping the date=YYYY-MM-DD folder in the stage."""
    local = partition_dir.resolve().as_posix()
    return (
        # both paths are quoted: the local path may contain spaces, the stage path contains "="
        f"put 'file://{local}/*.parquet' '@{schema}.{STAGE}/{partition_dir.name}/' "
        "overwrite = true auto_compress = false"
    )


def load_table_statements(schema: str, partitions: Sequence[str]) -> list[str]:
    """Create a session-only copy of the raw table and COPY the staged files for these dates into it."""
    tmp = f"{schema}.{RAW_TABLE}__LOAD"
    stmts = [f"create or replace temporary table {tmp} like {schema}.{RAW_TABLE}"]
    for part in partitions:
        stmts.append(
            f"copy into {tmp} from '@{schema}.{STAGE}/{part}/' "
            "file_format = (type = parquet use_logical_type = true) "
            "match_by_column_name = case_insensitive "
            # force: the temp table is new every run, so always read the files even if loaded before
            "force = true on_error = abort_statement"
        )
    return stmts


def merge_statement(schema: str) -> str:
    """Upsert the freshly copied rows into the permanent raw table."""
    target, tmp = f"{schema}.{RAW_TABLE}", f"{schema}.{RAW_TABLE}__LOAD"
    on = " and ".join(f"t.{k} = s.{k}" for k in KEY)
    update = ", ".join(f"{c} = s.{c}" for c in COLUMNS if c not in KEY)
    cols = ", ".join(COLUMNS)
    vals = ", ".join(f"s.{c}" for c in COLUMNS)
    return f"""merge into {target} as t
using (
    select * from {tmp}
    qualify row_number() over (partition by {", ".join(KEY)} order by ingested_at desc) = 1
) as s
on {on}
when matched and s.ingested_at >= t.ingested_at then update set {update}
when not matched then insert ({cols}) values ({vals})"""


def connect(cfg: SnowflakeConfig):
    """Key-pair authentication: no password is stored anywhere (Snowflake's recommended method for services)."""
    import snowflake.connector  # imported lazily so DuckDB-only users don't need the package loaded

    return snowflake.connector.connect(
        account=cfg.account,
        user=cfg.user,
        private_key_file=cfg.private_key_path,
        private_key_file_pwd=cfg.private_key_passphrase,
        role=cfg.role,
        warehouse=cfg.warehouse,
        database=cfg.database,
        session_parameters={"QUERY_TAG": "aqi-pipeline:load"},
    )


def partitions_for(raw_dir: Path, days: Iterable[date]) -> list[Path]:
    return [p for d in sorted(set(days)) if (p := raw_dir / f"date={d.isoformat()}").is_dir()]


def load(raw_dir: Path, days: Iterable[date], cfg: SnowflakeConfig, conn=None) -> int:
    """Run the full PUT -> COPY -> MERGE sequence for the given days. Returns rows merged."""
    parts = partitions_for(raw_dir, days)
    if not parts:
        log.warning("no local partitions found for %s", list(days))
        return 0
    own = conn is None
    conn = conn or connect(cfg)
    schema = cfg.raw_schema
    try:
        cur = conn.cursor()
        for sql in ddl_statements(schema):
            cur.execute(sql)
        for p in parts:
            cur.execute(put_statement(schema, p))
        for sql in load_table_statements(schema, [p.name for p in parts]):
            cur.execute(sql)
        cur.execute(merge_statement(schema))
        inserted, updated = cur.fetchone()[:2]
        log.info("snowflake merge: %s inserted, %s updated (%d day partitions)", inserted, updated, len(parts))
        return int(inserted) + int(updated)
    finally:
        if own:
            conn.close()
