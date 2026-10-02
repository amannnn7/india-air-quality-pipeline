"""The level-shift data-quality model must flag a real step change once, and ignore normal noise."""

from datetime import date

import duckdb

from aqi_pipeline import sample, transform
from aqi_pipeline.load import write_partitions
from aqi_pipeline.report import group_shifts
from aqi_pipeline.settings import load_cities

STEP_DAY = date(2025, 3, 2)  # day 31 of the generated range


def test_step_change_is_flagged_once_and_noise_is_not(tmp_path):
    raw, db = tmp_path / "raw", tmp_path / "wh.duckdb"
    cities = load_cities()[:3]
    data = sample.generate(cities, date(2025, 1, 30), date(2025, 3, 31))
    stepped = cities[0].city_id
    for row in data[stepped]:
        if row["observed_at"].date() >= STEP_DAY:  # simulate an upstream change: particulates triple overnight
            for col in ("pm2_5", "pm10"):
                if row[col] is not None:
                    row[col] = round(row[col] * 3, 1)
    write_partitions(data, raw, "test", "r1")
    transform.run_dbt("build", raw_dir=raw, warehouse=db)

    con = duckdb.connect(str(db))
    rows = con.execute("select city_id, shift_date, shift_aqi, direction from mart_aqi_level_shifts").fetchall()
    con.close()

    assert [r[0] for r in rows] == [stepped]  # only the stepped city, and only once
    _, shift_date, shift_aqi, direction = rows[0]
    assert abs((shift_date - STEP_DAY).days) <= 1
    assert direction == "up" and shift_aqi >= 50


def test_grouping_marks_simultaneous_shifts():
    def s(day, city, before, after):
        return {
            "shift_date": day,
            "city_name": city,
            "before_avg_aqi": before,
            "after_avg_aqi": after,
            "shift_aqi": after - before,
        }

    shifts = [
        s("2026-09-08", "Hyderabad", 80, 140),
        s("2026-09-09", "Pune", 75, 135),
        s("2026-09-10", "Bengaluru", 70, 125),
        s("2026-07-14", "Delhi", 150, 90),
    ]
    groups = group_shifts(shifts)
    assert [g["date"] for g in groups] == ["2026-07-14", "2026-09-08"]
    assert len(groups[1]["shifts"]) == 3 and groups[1]["before"] == 75 and groups[1]["after"] == 133
