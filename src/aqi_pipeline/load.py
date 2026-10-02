"""LOAD: land raw rows as Parquet files in a date-partitioned folder (a tiny "data lake").

Layout:  data/raw/air_quality_hourly/date=2026-09-24/delhi.parquet

Why this shape?
* One file per (day, city) means re-running a day OVERWRITES the same file.
  Running the pipeline twice never creates duplicates -> the load is idempotent.
* `date=...` folders are "Hive partitions": DuckDB (and Spark, Athena, BigQuery)
  can skip whole folders when you filter on date.
* Raw files are never edited by hand. The warehouse can always be rebuilt from them.
"""

from __future__ import annotations

import logging
import os
from collections import defaultdict
from datetime import UTC, datetime
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

log = logging.getLogger(__name__)

SCHEMA = pa.schema(
    [
        ("city_id", pa.string()),
        ("observed_at", pa.timestamp("s")),  # local India time (Asia/Kolkata), no tz offset
        ("pm2_5", pa.float64()),
        ("pm10", pa.float64()),
        ("no2", pa.float64()),
        ("so2", pa.float64()),
        ("o3", pa.float64()),
        ("co", pa.float64()),  # still ug/m3 here; converted to mg/m3 in dbt staging
        ("source", pa.string()),
        ("ingested_at", pa.timestamp("s", tz="UTC")),
        ("run_id", pa.string()),
    ]
)


def write_partitions(rows_by_city: dict[str, list[dict]], raw_dir: Path, source: str, run_id: str) -> list[Path]:
    """Split each city's rows by calendar day and write one Parquet file per (day, city)."""
    ingested_at = datetime.now(UTC).replace(microsecond=0)
    written: list[Path] = []
    for city_id, rows in rows_by_city.items():
        by_day: dict[str, list[dict]] = defaultdict(list)
        for r in rows:
            by_day[r["observed_at"].date().isoformat()].append(r)
        for day, day_rows in sorted(by_day.items()):
            for r in day_rows:
                r.update(source=source, ingested_at=ingested_at, run_id=run_id)
            table = pa.Table.from_pylist(day_rows, schema=SCHEMA)
            out = raw_dir / f"date={day}" / f"{city_id}.parquet"
            out.parent.mkdir(parents=True, exist_ok=True)
            tmp = out.with_suffix(".parquet.tmp")
            pq.write_table(table, tmp, compression="zstd")
            os.replace(tmp, out)  # atomic swap: readers never see a half-written file
            written.append(out)
    log.info("landed %d parquet files under %s", len(written), raw_dir)
    return written
