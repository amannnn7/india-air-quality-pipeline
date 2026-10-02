"""The incremental fact model must give exactly the same answer as a full rebuild."""

from datetime import date

import duckdb

from aqi_pipeline import sample, transform
from aqi_pipeline.load import write_partitions
from aqi_pipeline.settings import load_cities

COLS = "daily_aqi_id, aqi, aqi_category, dominant_pollutant, aqi_change_vs_prev_day, aqi_7d_avg"


def _facts(db):
    con = duckdb.connect(str(db))
    rows = con.execute(f"select {COLS} from fct_city_daily_aqi order by daily_aqi_id").fetchall()
    con.close()
    return rows


def test_incremental_run_matches_full_refresh(tmp_path):
    raw, db = tmp_path / "raw", tmp_path / "wh.duckdb"
    cities = load_cities()[:3]

    # day 1-20 loaded, full build
    write_partitions(sample.generate(cities, date(2025, 1, 1), date(2025, 1, 20)), raw, "test", "r1")
    transform.run_dbt("build", raw_dir=raw, warehouse=db)

    # days 19-25 arrive (19-20 are corrections with different values) -> incremental run
    write_partitions(sample.generate(cities, date(2025, 1, 19), date(2025, 1, 25), seed=99), raw, "test", "r2")
    transform.run_dbt("run", "--select", "fct_city_daily_aqi", raw_dir=raw, warehouse=db)
    incremental = _facts(db)

    transform.run_dbt("run", "--select", "fct_city_daily_aqi", "--full-refresh", raw_dir=raw, warehouse=db)
    full = _facts(db)

    assert len(full) == 25 * 3
    assert incremental == full


def test_backfill_of_older_days_reaches_the_marts(tmp_path):
    """Regression: back-filling days OLDER than what is already built must not be skipped by the incremental filter."""
    raw, db = tmp_path / "raw", tmp_path / "wh.duckdb"
    cities = load_cities()[:2]
    write_partitions(sample.generate(cities, date(2025, 2, 1), date(2025, 2, 20)), raw, "test", "r1")
    transform.build(raw_dir=raw, warehouse=db)  # normal daily build

    write_partitions(sample.generate(cities, date(2025, 1, 1), date(2025, 1, 31)), raw, "test", "r2")  # backfill
    transform.build(raw_dir=raw, warehouse=db, full_refresh=True)

    con = duckdb.connect(str(db))
    sql = "select min(observed_date), count(distinct observed_date) from fct_city_daily_aqi"
    first, n = con.execute(sql).fetchone()
    con.close()
    assert first == date(2025, 1, 1) and n == 51
