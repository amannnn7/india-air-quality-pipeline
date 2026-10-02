"""Landing must be idempotent: running the same day twice gives the same files, no duplicates."""

from datetime import datetime

import pyarrow.parquet as pq

from aqi_pipeline.load import write_partitions


def _rows(day: int, hours: range) -> list[dict]:
    return [
        {
            "city_id": "delhi",
            "observed_at": datetime(2025, 11, day, h),
            "pm2_5": 50.0,
            "pm10": 90.0,
            "no2": 20.0,
            "so2": 5.0,
            "o3": 30.0,
            "co": 800.0,
        }
        for h in hours
    ]


def test_rows_are_split_into_one_file_per_day(tmp_path):
    rows = {"delhi": _rows(1, range(20, 24)) + _rows(2, range(0, 5))}
    written = write_partitions(rows, tmp_path, source="test", run_id="r1")
    assert sorted(p.relative_to(tmp_path).as_posix() for p in written) == [
        "date=2025-11-01/delhi.parquet",
        "date=2025-11-02/delhi.parquet",
    ]
    assert pq.read_table(tmp_path / "date=2025-11-02" / "delhi.parquet").num_rows == 5


def test_reloading_a_day_overwrites_instead_of_duplicating(tmp_path):
    write_partitions({"delhi": _rows(1, range(24))}, tmp_path, source="test", run_id="r1")
    write_partitions({"delhi": _rows(1, range(24))}, tmp_path, source="test", run_id="r2")
    files = list(tmp_path.glob("*/*.parquet"))
    assert len(files) == 1
    table = pq.read_table(files[0])
    assert table.num_rows == 24
    assert set(table.column("run_id").to_pylist()) == {"r2"}
    assert not list(tmp_path.glob("**/*.tmp"))  # no half-written leftovers
