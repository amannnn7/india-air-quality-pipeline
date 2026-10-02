"""Snowflake loader: the SQL it generates, the order it runs in, and that MERGE is idempotent.

The MERGE test runs against `fakesnow`, a local Snowflake emulator, so no account is needed.
"""

from datetime import date, datetime, timedelta

import fakesnow
import pytest
import sqlglot

from aqi_pipeline import snowflake_load as sl
from aqi_pipeline.load import write_partitions
from aqi_pipeline.settings import SnowflakeConfig

CFG = SnowflakeConfig("acct", "user", "key.p8", None, "ROLE", "WH", "AQI", "RAW")


def test_generated_sql_is_valid_snowflake():
    stmts = (
        sl.ddl_statements("RAW") + sl.load_table_statements("RAW", ["date=2026-09-24"]) + [sl.merge_statement("RAW")]
    )
    for sql in stmts:
        sqlglot.parse_one(sql, read="snowflake")  # raises on a syntax error


def test_put_keeps_the_date_folder_and_overwrites(tmp_path):
    sql = sl.put_statement("RAW", tmp_path / "date=2026-09-24")
    assert "'@RAW.AQI_LANDING/date=2026-09-24/'" in sql
    assert "overwrite = true" in sql


def test_copy_forces_reload_and_matches_columns_by_name():
    copies = sl.load_table_statements("RAW", ["date=2026-09-23", "date=2026-09-24"])[1:]
    assert len(copies) == 2
    for sql in copies:
        assert "force = true" in sql and "match_by_column_name = case_insensitive" in sql
        assert "use_logical_type = true" in sql  # read Parquet timestamps as timestamps, not integers


class RecordingCursor:
    def __init__(self):
        self.sql: list[str] = []

    def execute(self, sql):
        self.sql.append(" ".join(sql.split()))

    def fetchone(self):
        return (240, 0)


class RecordingConnection:
    def __init__(self):
        self.cur = RecordingCursor()

    def cursor(self):
        return self.cur

    def close(self):
        pass


def test_load_runs_put_copy_merge_in_order(tmp_path):
    rows = [
        {
            "city_id": "delhi",
            "observed_at": datetime(2026, 9, d, h),
            "pm2_5": 50.0,
            "pm10": 90.0,
            "no2": 20.0,
            "so2": 5.0,
            "o3": 30.0,
            "co": 800.0,
        }
        for d in (23, 24)
        for h in range(24)
    ]
    write_partitions({"delhi": rows}, tmp_path, source="test", run_id="r1")
    conn = RecordingConnection()
    merged = sl.load(tmp_path, [date(2026, 9, 23), date(2026, 9, 24)], CFG, conn=conn)
    kinds = [s.split()[0].lower() for s in conn.cur.sql]
    assert kinds == ["create", "create", "create", "put", "put", "create", "copy", "copy", "merge"]
    assert merged == 240


def test_load_skips_days_without_local_files(tmp_path):
    assert sl.load(tmp_path, [date(2026, 1, 1)], CFG, conn=RecordingConnection()) == 0


@pytest.fixture
def snowflake_emulator():
    with fakesnow.patch(create_database_on_connect=True, create_schema_on_connect=True):
        import snowflake.connector

        conn = snowflake.connector.connect(database="AQI", schema="RAW")
        yield conn
        conn.close()


def test_merge_is_an_idempotent_upsert(snowflake_emulator):
    cur = snowflake_emulator.cursor()
    for sql in sl.ddl_statements("RAW")[:2]:
        cur.execute(sql)
    # The emulator only supports unqualified temporary tables; real Snowflake uses RAW.AIR_QUALITY_HOURLY__LOAD.
    tmp = "AIR_QUALITY_HOURLY__LOAD"
    merge = sl.merge_statement("RAW").replace(f"RAW.{tmp}", tmp)
    cur.execute(f"create or replace temporary table {tmp} like RAW.AIR_QUALITY_HOURLY")

    t0 = datetime(2026, 9, 24)
    rows = [
        (
            "delhi",
            t0 + timedelta(hours=h),
            50.0,
            90.0,
            20.0,
            5.0,
            30.0,
            800.0,
            "test",
            "2026-09-25 01:00:00+00:00",
            "r1",
        )
        for h in range(24)
    ]
    cur.executemany(f"insert into {tmp} values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)", rows)

    cur.execute(merge)
    assert tuple(int(x) for x in cur.fetchone()[:2]) == (24, 0)  # first load: 24 inserted
    cur.execute(merge)
    assert tuple(int(x) for x in cur.fetchone()[:2]) == (0, 24)  # same data again: updated, not duplicated

    cur.execute("select count(*) from RAW.AIR_QUALITY_HOURLY")
    assert cur.fetchone()[0] == 24
