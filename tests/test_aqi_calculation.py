"""End-to-end check of the AQI maths: controlled raw data -> real dbt build -> known answers.

Day 1 (clean day, all 24 hours):
    PM2.5 45  -> 50 + (45-30)*50/30 = 75
    PM10  80  -> 50 + (80-50)*50/50 = 80   <- highest, so AQI = 80, dominant = PM10
    NO2   30  -> 30*50/40 = 37.5 -> 38
    O3 40, CO 0.5 mg/m3 -> 40 and 25
Day 2 (only 10 hours of data): 24-hour averages are invalid (< 16 h), so no AQI is published.
Day 3 (PM2.5 400, above the top breakpoint): the sub-index is capped at 500, category Severe.
"""

from datetime import datetime

import duckdb
import pytest

from aqi_pipeline import transform
from aqi_pipeline.load import write_partitions


def _day(day: int, hours: range, pm25: float) -> list[dict]:
    return [
        {
            "city_id": "delhi",
            "observed_at": datetime(2025, 1, day, h),
            "pm2_5": pm25,
            "pm10": 80.0,
            "no2": 30.0,
            "so2": 10.0,
            "o3": 40.0,
            "co": 500.0,
        }
        for h in hours
    ]


@pytest.fixture(scope="module")
def warehouse(tmp_path_factory):
    tmp = tmp_path_factory.mktemp("e2e")
    raw, db = tmp / "raw", tmp / "wh.duckdb"
    rows = {"delhi": _day(1, range(24), 45.0) + _day(2, range(10), 45.0) + _day(3, range(24), 400.0)}
    write_partitions(rows, raw, source="test", run_id="t")
    transform.run_dbt("build", raw_dir=raw, warehouse=db)  # raises if any model or test fails
    con = duckdb.connect(str(db))  # same config as dbt's in-process connection
    yield con
    con.close()


def _day_row(con, d: str) -> dict:
    cur = con.execute("select * from fct_city_daily_aqi where observed_date = ?", [d])
    cols = [c[0] for c in cur.description]
    return dict(zip(cols, cur.fetchone(), strict=True))


def test_normal_day_uses_highest_sub_index(warehouse):
    r = _day_row(warehouse, "2025-01-01")
    assert (r["pm2_5_sub_index"], r["pm10_sub_index"], r["no2_sub_index"]) == (75, 80, 38)
    assert (r["o3_sub_index"], r["co_sub_index"]) == (40, 25)
    assert r["aqi"] == 80
    assert r["dominant_pollutant"] == "pm10"
    assert r["aqi_category"] == "Satisfactory"


def test_thin_day_gets_no_aqi(warehouse):
    r = _day_row(warehouse, "2025-01-02")
    assert r["hours_reported"] == 10
    assert r["aqi"] is None and r["is_aqi_valid"] is False


def test_extreme_value_is_capped_at_500(warehouse):
    r = _day_row(warehouse, "2025-01-03")
    assert r["pm2_5_sub_index"] == 500
    assert r["aqi"] == 500 and r["aqi_category"] == "Severe"
