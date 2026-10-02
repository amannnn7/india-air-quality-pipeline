"""Extraction logic, tested without touching the network (the HTTP call is faked)."""

from datetime import date

import pytest

from aqi_pipeline import extract
from aqi_pipeline.settings import City

DELHI = City("delhi", "Delhi", 28.6139, 77.2090)


class FakeResponse:
    def __init__(self, payload, status=200):
        self._payload, self.status_code = payload, status

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeSession:
    def __init__(self, payload, status=200):
        self.payload, self.status, self.calls = payload, status, []

    def get(self, url, params, timeout):
        self.calls.append(params)
        return FakeResponse(self.payload, self.status)


def test_fetch_sends_the_right_request(delhi_payload):
    session = FakeSession(delhi_payload)
    extract.fetch_city(session, DELHI, date(2025, 11, 1), date(2025, 11, 1))
    params = session.calls[0]
    assert params["timezone"] == "Asia/Kolkata"
    assert params["start_date"] == params["end_date"] == "2025-11-01"
    assert set(params["hourly"].split(",")) == {
        "pm2_5",
        "pm10",
        "nitrogen_dioxide",
        "sulphur_dioxide",
        "ozone",
        "carbon_monoxide",
    }


def test_rows_are_one_per_hour_with_renamed_columns(delhi_payload):
    payload = extract.fetch_city(FakeSession(delhi_payload), DELHI, date(2025, 11, 1), date(2025, 11, 1))
    rows = extract.to_rows(DELHI, payload)
    assert len(rows) == 24
    assert set(rows[0]) == {"city_id", "observed_at", "pm2_5", "pm10", "no2", "so2", "o3", "co"}
    assert rows[0]["city_id"] == "delhi"
    assert rows[0]["observed_at"].hour == 0


def test_one_failing_city_fails_the_run_with_its_name(monkeypatch, delhi_payload):
    monkeypatch.setattr(extract, "make_session", lambda: FakeSession(delhi_payload, status=503))
    with pytest.raises(RuntimeError, match="delhi"):
        extract.extract([DELHI], date(2025, 11, 1), date(2025, 11, 1), pause_s=0)
