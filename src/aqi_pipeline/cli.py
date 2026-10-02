"""Command-line entry point. Every step can run alone, or `run` does the whole daily job.

    aqi run                  daily job: last N finished days -> land -> (Snowflake load) -> dbt -> docs -> report
    aqi ingest --days 3      only extract + land (+ load into Snowflake); used as the first Airflow task
    aqi backfill --start 2026-07-01 --end 2026-09-24
    aqi sample --days 60     offline demo with synthetic data on DuckDB (no internet, no Snowflake)
    aqi transform            only dbt build + source freshness
    aqi report               only rebuild dbt docs + the dashboard
    aqi snowflake-keygen     create the RSA key pair for Snowflake key-pair authentication

The warehouse is chosen with the AQI_TARGET environment variable: "snowflake" or "duckdb" (default).
"""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

from . import extract, load, report, sample, snowflake_load, transform
from .settings import (
    RAW_DIR,
    SAMPLE_RAW_DIR,
    SAMPLE_WAREHOUSE_PATH,
    SITE_DIR,
    TARGET,
    TIMEZONE,
    WAREHOUSE_PATH,
    SnowflakeConfig,
    load_cities,
)

log = logging.getLogger("aqi")


def _run_id() -> str:
    return datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")


def _today_india() -> date:
    return datetime.now(ZoneInfo(TIMEZONE)).date()


def _date_chunks(start: date, end: date, size_days: int = 31):
    """Split a long backfill into smaller API calls."""
    cur = start
    while cur <= end:
        stop = min(cur + timedelta(days=size_days - 1), end)
        yield cur, stop
        cur = stop + timedelta(days=1)


def ingest(start: date, end: date, target: str) -> None:
    """Extract -> validate -> land as Parquet -> (if Snowflake) PUT/COPY/MERGE into RAW."""
    if end >= _today_india():
        raise SystemExit(f"end date {end} is not a finished day yet; use yesterday or earlier")
    cities = load_cities()
    run_id = _run_id()
    for s, e in _date_chunks(start, end):
        log.info("extracting %s -> %s for %d cities", s, e, len(cities))
        rows = extract.extract(cities, s, e)
        load.write_partitions(rows, RAW_DIR, source="open-meteo:cams-global", run_id=run_id)
    if target == "snowflake":
        days = [start + timedelta(days=i) for i in range((end - start).days + 1)]
        snowflake_load.load(RAW_DIR, days, SnowflakeConfig.from_env())


def keygen(out_dir: Path) -> None:
    """Generate a 2048-bit RSA key pair in the format Snowflake expects (PKCS#8 private key)."""
    from cryptography.hazmat.primitives import serialization
    from cryptography.hazmat.primitives.asymmetric import rsa

    out_dir.mkdir(parents=True, exist_ok=True)
    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    private_pem = key.private_bytes(
        serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
    )
    public_pem = key.public_key().public_bytes(
        serialization.Encoding.PEM, serialization.PublicFormat.SubjectPublicKeyInfo
    )
    (out_dir / "rsa_key.p8").write_bytes(private_pem)
    (out_dir / "rsa_key.pub").write_bytes(public_pem)
    body = "".join(line for line in public_pem.decode().splitlines() if "PUBLIC KEY" not in line)
    print(f"Private key: {out_dir / 'rsa_key.p8'}   (keep it secret - never commit it)")
    print("Paste this public key into snowflake/setup.sql where it says <PASTE_PUBLIC_KEY_HERE>:\n")
    print(body)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s: %(message)s")
    p = argparse.ArgumentParser(prog="aqi", description="India air-quality data pipeline")
    sub = p.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="daily job: re-load the last N finished days, transform, report")
    run.add_argument("--days", type=int, default=3, help="how many finished days to (re)load (default 3)")

    ing = sub.add_parser("ingest", help="only extract + land (+ load into Snowflake) the last N finished days")
    ing.add_argument("--days", type=int, default=3)

    bf = sub.add_parser("backfill", help="load a historical date range, then transform and report")
    bf.add_argument("--start", type=date.fromisoformat, required=True)
    bf.add_argument("--end", type=date.fromisoformat, required=True)

    sm = sub.add_parser("sample", help="offline demo on DuckDB with synthetic data")
    sm.add_argument("--days", type=int, default=60)

    sub.add_parser("transform", help="run dbt build + source freshness")
    sub.add_parser("report", help="rebuild dbt docs and the dashboard from the warehouse")

    kg = sub.add_parser("snowflake-keygen", help="create an RSA key pair for Snowflake key-pair auth")
    kg.add_argument("--out", type=Path, default=Path("keys"))

    args = p.parse_args(argv)

    if args.command == "snowflake-keygen":
        keygen(args.out)
        return 0

    target, raw_dir, warehouse = TARGET, RAW_DIR, WAREHOUSE_PATH
    if target not in {"duckdb", "snowflake"}:
        p.error(f"AQI_TARGET must be 'duckdb' or 'snowflake', got '{target}'")

    if args.command in {"run", "ingest"}:
        end = _today_india() - timedelta(days=1)
        # Re-loading more than one day catches late corrections from the data provider.
        ingest(end - timedelta(days=args.days - 1), end, target)
    elif args.command == "backfill":
        if args.start > args.end:
            p.error("--start must be on or before --end")
        ingest(args.start, args.end, target)
    elif args.command == "sample":
        # Synthetic data always goes to its own local DuckDB, never to Snowflake.
        target, raw_dir, warehouse = "duckdb", SAMPLE_RAW_DIR, SAMPLE_WAREHOUSE_PATH
        end = _today_india() - timedelta(days=1)
        rows = sample.generate(load_cities(), end - timedelta(days=args.days - 1), end)
        load.write_partitions(rows, raw_dir, source="synthetic_sample", run_id=_run_id())

    kw = {"target": target, "raw_dir": raw_dir, "warehouse": warehouse}
    if args.command in {"run", "backfill", "sample", "transform"}:
        transform.build(**kw)
    if args.command in {"run", "backfill", "sample", "report"}:
        transform.docs(SITE_DIR, **kw)
        # print, not log: dbt reconfigures Python logging while it runs
        print(f"Dashboard ready: {report.build(**kw)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
