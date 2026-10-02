"""Data contract for the Open-Meteo air-quality response.

If the API ever changes shape (a field renamed, arrays of different lengths,
wrong units), we want the pipeline to STOP loudly here instead of silently
writing bad data that shows up as a wrong number on a dashboard weeks later.
"""

from __future__ import annotations

from datetime import datetime

from pydantic import BaseModel, Field, field_validator, model_validator

from .settings import POLLUTANTS

EXPECTED_UNITS = {
    "pm2_5": "μg/m³",
    "pm10": "μg/m³",
    "nitrogen_dioxide": "μg/m³",
    "sulphur_dioxide": "μg/m³",
    "ozone": "μg/m³",
    "carbon_monoxide": "μg/m³",
}


class HourlyBlock(BaseModel):
    time: list[datetime]
    pm2_5: list[float | None]
    pm10: list[float | None]
    nitrogen_dioxide: list[float | None]
    sulphur_dioxide: list[float | None]
    ozone: list[float | None]
    carbon_monoxide: list[float | None]

    @model_validator(mode="after")
    def arrays_same_length(self) -> HourlyBlock:
        n = len(self.time)
        for name in POLLUTANTS:
            if len(getattr(self, name)) != n:
                raise ValueError(f"'{name}' has {len(getattr(self, name))} values but 'time' has {n}")
        return self

    @field_validator("pm2_5", "pm10", "nitrogen_dioxide", "sulphur_dioxide", "ozone", "carbon_monoxide")
    @classmethod
    def no_negative_concentrations(cls, values: list[float | None]) -> list[float | None]:
        if any(v is not None and v < 0 for v in values):
            raise ValueError("negative concentration found")
        return values


class AirQualityResponse(BaseModel):
    latitude: float
    longitude: float
    timezone: str
    hourly_units: dict[str, str]
    hourly: HourlyBlock
    generationtime_ms: float | None = Field(default=None)

    @field_validator("hourly_units")
    @classmethod
    def units_are_expected(cls, units: dict[str, str]) -> dict[str, str]:
        for var, expected in EXPECTED_UNITS.items():
            got = units.get(var)
            if got is None:
                raise ValueError(f"unit for '{var}' missing from response")
            # Normalise the two ways the micro sign can be encoded.
            if got.replace("µ", "μ") != expected:
                raise ValueError(f"unit for '{var}' is '{got}', expected '{expected}'")
        return units
