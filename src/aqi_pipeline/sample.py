"""Offline SAMPLE data, so the whole pipeline can be demoed and tested without internet.

The numbers are synthetic (made up), but shaped like real Indian air quality:
north-Indian cities are dirtier, pollution rises at night and early morning,
and it climbs as the season moves towards winter. Every row is tagged
source='synthetic_sample' and the dashboard shows a banner when it is used,
so sample data can never be mistaken for real data.
"""

from __future__ import annotations

import math
import random
from datetime import date, datetime, timedelta

from .settings import City

# rough "how polluted is this city" multiplier
CITY_LEVEL = {
    "delhi": 2.6,
    "lucknow": 2.2,
    "patna": 2.3,
    "kolkata": 1.7,
    "ahmedabad": 1.5,
    "mumbai": 1.1,
    "pune": 1.0,
    "hyderabad": 0.95,
    "bengaluru": 0.75,
    "chennai": 0.8,
}


def generate(cities: list[City], start: date, end: date, seed: int = 7) -> dict[str, list[dict]]:
    rng = random.Random(seed)
    out: dict[str, list[dict]] = {}
    days = (end - start).days + 1
    for city in cities:
        level = CITY_LEVEL.get(city.city_id, 1.2)
        rows = []
        weather = 1.0
        for d in range(days):
            day = start + timedelta(days=d)
            season = 1 + 0.9 * max(0.0, (day.timetuple().tm_yday - 240) / 120)  # rises from Sept to winter
            weather = 0.7 * weather + 0.3 * rng.uniform(0.6, 1.5)  # multi-day weather spells
            for h in range(24):
                diurnal = 1 + 0.35 * math.cos((h - 2) / 24 * 2 * math.pi)  # peaks around 2 am
                base = level * season * weather * diurnal
                noise = lambda s=0.12: max(0.0, rng.gauss(1, s))  # noqa: E731
                rows.append(
                    {
                        "city_id": city.city_id,
                        "observed_at": datetime(day.year, day.month, day.day, h),
                        "pm2_5": round(28 * base * noise(), 1),
                        "pm10": round(52 * base * noise(), 1),
                        "no2": round(18 * base * noise(), 1),
                        "so2": round(7 * base * noise(), 1),
                        "o3": round(45 * (1.6 - 0.6 * diurnal) * noise(0.2), 1),  # ozone peaks in daytime
                        "co": round(420 * base * noise(), 1),  # ug/m3, like the real API
                    }
                )
        # a couple of missing hours, like real feeds have
        for _ in range(3):
            rows[rng.randrange(len(rows))]["no2"] = None
        out[city.city_id] = rows
    return out
