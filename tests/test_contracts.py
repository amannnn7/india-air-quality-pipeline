"""The data contract must accept good responses and reject broken ones loudly."""

import copy

import pytest
from pydantic import ValidationError

from aqi_pipeline.contracts import AirQualityResponse


def test_valid_response_passes(delhi_payload):
    resp = AirQualityResponse.model_validate(delhi_payload)
    assert len(resp.hourly.time) == 24
    assert resp.hourly.nitrogen_dioxide[-1] is None  # nulls are allowed


def test_arrays_of_different_length_are_rejected(delhi_payload):
    bad = copy.deepcopy(delhi_payload)
    bad["hourly"]["pm10"] = bad["hourly"]["pm10"][:-1]
    with pytest.raises(ValidationError, match="pm10"):
        AirQualityResponse.model_validate(bad)


def test_negative_concentration_is_rejected(delhi_payload):
    bad = copy.deepcopy(delhi_payload)
    bad["hourly"]["pm2_5"][0] = -5
    with pytest.raises(ValidationError, match="negative"):
        AirQualityResponse.model_validate(bad)


def test_unexpected_unit_is_rejected(delhi_payload):
    bad = copy.deepcopy(delhi_payload)
    bad["hourly_units"]["carbon_monoxide"] = "mg/m³"
    with pytest.raises(ValidationError, match="carbon_monoxide"):
        AirQualityResponse.model_validate(bad)


def test_missing_pollutant_is_rejected(delhi_payload):
    bad = copy.deepcopy(delhi_payload)
    del bad["hourly"]["ozone"]
    with pytest.raises(ValidationError):
        AirQualityResponse.model_validate(bad)
