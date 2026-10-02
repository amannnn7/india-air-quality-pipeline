"""REPORT: read the finished marts from the warehouse (Snowflake or DuckDB) and render a static HTML dashboard.

The output (site/index.html) is a single file: GitHub Pages serves it for free,
and it also opens straight from your disk. It includes a "pipeline health"
panel built from dbt's own run artefacts, so viewers can see the data was tested.
"""

from __future__ import annotations

import json
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path

from jinja2 import Environment, PackageLoader, select_autoescape

from .settings import DBT_DIR, RAW_DIR, SITE_DIR, WAREHOUSE_PATH
from .warehouse import Warehouse

log = logging.getLogger(__name__)

POLLUTANT_LABELS = {"pm2_5": "PM2.5", "pm10": "PM10", "no2": "NO₂", "so2": "SO₂", "o3": "O₃", "co": "CO"}


def _dbt_health() -> dict:
    """Summarise the last `dbt build` and `dbt source freshness` from dbt's JSON artefacts."""
    health: dict = {"tests_total": 0, "tests_passed": 0, "models": 0, "failed": [], "freshness": "unknown"}
    run_results = DBT_DIR / "target" / "build_results.json"  # saved copy of the `dbt build` results
    if run_results.exists():
        rr = json.loads(run_results.read_text(encoding="utf-8"))
        for r in rr.get("results", []):
            uid = r["unique_id"]
            if uid.startswith("test."):
                health["tests_total"] += 1
                if r["status"] == "pass":
                    health["tests_passed"] += 1
                else:
                    health["failed"].append(uid.split(".")[2])
            elif uid.startswith("model."):
                health["models"] += 1
        health["built_at"] = rr["metadata"]["generated_at"][:19].replace("T", " ") + " UTC"
        health["elapsed_s"] = round(rr.get("elapsed_time", 0), 1)
    sources = DBT_DIR / "target" / "sources.json"
    if sources.exists():
        res = json.loads(sources.read_text(encoding="utf-8")).get("results", [])
        if res:
            health["freshness"] = res[0]["status"]  # pass / warn / error
            health["max_loaded_at"] = str(res[0].get("max_loaded_at", ""))[:19].replace("T", " ") + " UTC"
    return health


def build(
    target: str = "duckdb", raw_dir: Path = RAW_DIR, warehouse: Path = WAREHOUSE_PATH, site_dir: Path = SITE_DIR
) -> Path:
    wh = Warehouse(target, warehouse)
    summary = wh.query("select * from mart_city_aqi_summary order by pollution_rank_30d")
    last_day = wh.query("select max(observed_date) as d from fct_city_daily_aqi")[0]["d"]
    cutoff = (last_day - timedelta(days=90)).isoformat()  # computed in Python: portable across warehouses
    daily = wh.query(
        f"""
        select f.city_id, c.city_name, cast(f.observed_date as varchar) as d, f.aqi, f.aqi_category,
               f.dominant_pollutant, f.aqi_7d_avg
        from fct_city_daily_aqi f
        join dim_city c on c.city_id = f.city_id
        where f.observed_date > '{cutoff}'
        order by f.observed_date, c.city_name
        """
    )
    stats = wh.query(
        """
        select count(*) as hourly_rows,
               count(distinct city_id) as cities,
               min(observed_date) as first_day,
               max(observed_date) as last_day,
               count(distinct observed_date) as days,
               max(case when source = 'synthetic_sample' then 1 else 0 end) as has_synthetic
        from stg_air_quality__hourly
        """
    )[0]
    warehouse_label = wh.label
    wh.close()

    stats["first_day"] = stats["first_day"].strftime("%d %b %Y")
    stats["last_day"] = stats["last_day"].strftime("%d %b %Y")
    for row in daily:
        row["aqi_7d_avg"] = float(row["aqi_7d_avg"]) if row["aqi_7d_avg"] is not None else None
    for row in summary:
        if row.get("latest_date") is not None:
            row["latest_date"] = row["latest_date"].isoformat()

    env = Environment(loader=PackageLoader("aqi_pipeline", "templates"), autoescape=select_autoescape())
    env.filters["pollutant"] = lambda p: POLLUTANT_LABELS.get(p, "–")
    html = env.get_template("dashboard.html.j2").render(
        summary=summary,
        daily=daily,
        stats=stats,
        health=_dbt_health(),
        warehouse=warehouse_label,
        has_docs=(site_dir / "dbt-docs" / "index.html").exists(),
        raw_files=sum(1 for _ in raw_dir.glob("*/*.parquet")),
        generated_at=datetime.now(UTC).strftime("%d %b %Y, %H:%M UTC"),
    )
    site_dir.mkdir(parents=True, exist_ok=True)
    out = site_dir / "index.html"
    out.write_text(html, encoding="utf-8")
    log.info("dashboard written to %s", out)
    return out
