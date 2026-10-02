"""TRANSFORM: run dbt (seeds -> models -> tests) in the chosen warehouse, then build dbt docs."""

from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path

from dbt.cli.main import dbtRunner

from .settings import DBT_DIR, DBT_TARGET, RAW_DIR, WAREHOUSE_PATH

log = logging.getLogger(__name__)


def run_dbt(*args: str, target: str = "duckdb", raw_dir: Path = RAW_DIR, warehouse: Path = WAREHOUSE_PATH) -> None:
    """Invoke dbt in-process. Raises if any model fails or any test fails."""
    # profiles.yml and the DuckDB raw-view macro read these environment variables
    os.environ["AQI_DBT_TARGET"] = DBT_TARGET[target]
    os.environ["AQI_WAREHOUSE"] = str(warehouse)
    os.environ["AQI_RAW_DIR"] = raw_dir.as_posix()
    if target == "duckdb":
        warehouse.parent.mkdir(parents=True, exist_ok=True)
    cli_args = [*args, "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)]
    log.info("dbt %s (target=%s)", " ".join(args), DBT_TARGET[target])
    result = dbtRunner().invoke(cli_args)
    if not result.success:
        raise RuntimeError(f"dbt {' '.join(args)} failed - see the log above for the failing model/test")


def build(
    target: str = "duckdb", raw_dir: Path = RAW_DIR, warehouse: Path = WAREHOUSE_PATH, full_refresh: bool = False
) -> None:
    """full_refresh=True rebuilds the incremental model from all raw data. A backfill needs it: the normal
    incremental run only re-processes the last few days, so older back-filled days would never reach the marts."""
    kw = {"target": target, "raw_dir": raw_dir, "warehouse": warehouse}
    # `dbt build` = load seeds, build every model, and run every test, in dependency order.
    # If a test on an upstream model fails, the models downstream of it are skipped.
    run_dbt("build", *(["--full-refresh"] if full_refresh else []), **kw)
    # keep this run's results: later dbt commands (docs, freshness) overwrite run_results.json
    shutil.copyfile(DBT_DIR / "target" / "run_results.json", DBT_DIR / "target" / "build_results.json")
    # Freshness check: warns/fails if the newest raw data is too old (e.g. the API stopped updating).
    run_dbt("source", "freshness", **kw)


def docs(site_dir: Path, target: str = "duckdb", raw_dir: Path = RAW_DIR, warehouse: Path = WAREHOUSE_PATH) -> Path:
    """Generate dbt's documentation site (model descriptions, columns, tests, lineage graph) as ONE html file."""
    run_dbt("docs", "generate", "--static", target=target, raw_dir=raw_dir, warehouse=warehouse)
    out = site_dir / "dbt-docs" / "index.html"
    out.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(DBT_DIR / "target" / "static_index.html", out)
    return out
