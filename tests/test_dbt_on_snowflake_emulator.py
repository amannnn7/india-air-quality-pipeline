"""Build every dbt model and run every dbt test with the Snowflake adapter (target=prod),
against `fakesnow`, a local Snowflake emulator. This proves the SQL runs in Snowflake's dialect
(MAX_BY, DATEADD, MODE, QUALIFY ...) without needing an account.

Emulator limitations handled here: it cannot run dbt's seed loader (PUT) or Snowflake's
PUT/COPY for Parquet, so seeds and raw rows are inserted directly. The real loader SQL is
covered by test_snowflake_load.py.
"""

import csv
from datetime import date

import fakesnow
import pytest

from aqi_pipeline import sample, transform
from aqi_pipeline.cli import keygen
from aqi_pipeline.settings import DBT_DIR, load_cities

SEEDS = {
    "cities": "city_id varchar, city_name varchar, state varchar, region varchar, latitude double, longitude double",
    "aqi_breakpoints": "pollutant varchar, category varchar, index_low double, index_high double, "
    "conc_low double, conc_high double, unit varchar, averaging varchar",
}


def test_dbt_build_on_snowflake_dialect(tmp_path, monkeypatch):
    keygen(tmp_path / "keys")  # dbt-snowflake reads the key file even though the emulator ignores it
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "emulator")
    monkeypatch.setenv("SNOWFLAKE_USER", "aqi")
    monkeypatch.setenv("SNOWFLAKE_PRIVATE_KEY_PATH", str(tmp_path / "keys" / "rsa_key.p8"))

    with fakesnow.patch(create_database_on_connect=True, create_schema_on_connect=True):
        import snowflake.connector

        conn = snowflake.connector.connect(database="AQI", schema="RAW")
        cur = conn.cursor()
        cur.execute("create schema if not exists AQI.REF")
        for name, cols in SEEDS.items():
            cur.execute(f"create or replace table AQI.REF.{name} ({cols})")
            rows = list(csv.reader(open(DBT_DIR / "seeds" / f"{name}.csv", encoding="utf-8")))[1:]
            cur.executemany(f"insert into AQI.REF.{name} values ({','.join(['%s'] * len(rows[0]))})", rows)

        cur.execute(
            "create table AQI.RAW.AIR_QUALITY_HOURLY (city_id varchar, observed_at timestamp_ntz, pm2_5 float, "
            "pm10 float, no2 float, so2 float, o3 float, co float, source varchar, ingested_at timestamp_tz, "
            "run_id varchar)"
        )
        data = sample.generate(load_cities()[:4], date(2025, 1, 1), date(2025, 1, 10))
        rows = [
            (
                r["city_id"],
                r["observed_at"],
                r["pm2_5"],
                r["pm10"],
                r["no2"],
                r["so2"],
                r["o3"],
                r["co"],
                "test",
                "2025-01-11 00:00:00+00:00",
                "r1",
            )
            for city_rows in data.values()
            for r in city_rows
        ]
        cur.executemany("insert into AQI.RAW.AIR_QUALITY_HOURLY values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", rows)
        conn.commit()

        # threads=1: the emulator does not support concurrent DDL
        transform.run_dbt("build", "--threads", "1", "--exclude", "resource_type:seed", target="snowflake")

        cur.execute("select count(*), count(distinct daily_aqi_id) from AQI.ANALYTICS.FCT_CITY_DAILY_AQI")
        assert cur.fetchone() == (40, 40)
        cur.execute("select count(*) from AQI.ANALYTICS.MART_CITY_AQI_SUMMARY where latest_aqi is not null")
        assert cur.fetchone()[0] == 4
        conn.close()


@pytest.fixture(autouse=True)
def _restore_dbt_target(monkeypatch):
    # run_dbt sets AQI_DBT_TARGET; make sure other tests go back to DuckDB
    yield
    monkeypatch.setenv("AQI_DBT_TARGET", "dev")
