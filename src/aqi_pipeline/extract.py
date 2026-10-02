"""EXTRACT: pull hourly pollutant readings for each city from the Open-Meteo API."""

from __future__ import annotations

import logging
import time
from datetime import date

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from .contracts import AirQualityResponse
from .settings import API_URL, POLLUTANTS, TIMEZONE, City

log = logging.getLogger(__name__)


def make_session() -> requests.Session:
    """A session that retries on network blips and on 429/5xx, with growing waits (1s, 2s, 4s...)."""
    retry = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    session.headers["User-Agent"] = "aqi-pipeline/1.0 (portfolio project)"
    return session


def fetch_city(session: requests.Session, city: City, start: date, end: date) -> AirQualityResponse:
    """Download one city's hourly data for [start, end] and validate it against the contract."""
    params = {
        "latitude": city.latitude,
        "longitude": city.longitude,
        "hourly": ",".join(POLLUTANTS),
        "timezone": TIMEZONE,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
    }
    resp = session.get(API_URL, params=params, timeout=30)
    resp.raise_for_status()
    return AirQualityResponse.model_validate(resp.json())


def to_rows(city: City, payload: AirQualityResponse) -> list[dict]:
    """Turn the API's column-oriented arrays into one row per (city, hour)."""
    h = payload.hourly
    rows = []
    for i, ts in enumerate(h.time):
        row = {"city_id": city.city_id, "observed_at": ts}
        for api_name, col in POLLUTANTS.items():
            row[col] = getattr(h, api_name)[i]
        rows.append(row)
    return rows


def extract(cities: list[City], start: date, end: date, pause_s: float = 0.5) -> dict[str, list[dict]]:
    """Fetch every city. One city failing does not hide the others: we collect errors and raise at the end."""
    session = make_session()
    results: dict[str, list[dict]] = {}
    errors: dict[str, str] = {}
    for city in cities:
        try:
            payload = fetch_city(session, city, start, end)
            results[city.city_id] = to_rows(city, payload)
            log.info("extracted %-10s %4d hourly rows", city.city_id, len(results[city.city_id]))
        except Exception as exc:  # noqa: BLE001 - we re-raise below with full context
            errors[city.city_id] = f"{type(exc).__name__}: {exc}"
            log.error("failed %s: %s", city.city_id, errors[city.city_id])
        time.sleep(pause_s)  # be polite to a free API
    if errors:
        raise RuntimeError(f"extraction failed for {len(errors)} city(ies): {errors}")
    return results
