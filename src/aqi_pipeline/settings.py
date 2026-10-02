"""Central place for paths and settings.

Everything can be overridden with environment variables, so the same code runs
on a laptop, in GitHub Actions, in Docker or inside Airflow without edits.
"""

from __future__ import annotations

import csv
import os
from dataclasses import dataclass
from pathlib import Path


def _find_repo_root() -> Path:
    """Walk up from this file until we find pyproject.toml (the repo root)."""
    here = Path(__file__).resolve()
    for parent in here.parents:
        if (parent / "pyproject.toml").exists():
            return parent
    return Path.cwd()


REPO_ROOT = Path(os.getenv("AQI_REPO_ROOT", _find_repo_root()))
DBT_DIR = REPO_ROOT / "dbt"
RAW_DIR = Path(os.getenv("AQI_RAW_DIR", REPO_ROOT / "data" / "raw" / "air_quality_hourly"))
WAREHOUSE_PATH = Path(os.getenv("AQI_WAREHOUSE", REPO_ROOT / "data" / "warehouse.duckdb"))
SITE_DIR = Path(os.getenv("AQI_SITE_DIR", REPO_ROOT / "site"))
CITIES_CSV = DBT_DIR / "seeds" / "cities.csv"

# `aqi sample` uses its own folders so synthetic data can never mix with (or be committed as) real data.
SAMPLE_RAW_DIR = REPO_ROOT / "data" / "sample" / "raw" / "air_quality_hourly"
SAMPLE_WAREHOUSE_PATH = REPO_ROOT / "data" / "sample" / "warehouse.duckdb"

# Which warehouse to use:
#   "snowflake" -> production: raw data is loaded into Snowflake and dbt builds there
#   "duckdb"    -> local development and CI tests: free, offline, no credentials needed
TARGET = os.getenv("AQI_TARGET", "duckdb").lower()
DBT_TARGET = {"duckdb": "dev", "snowflake": "prod"}


@dataclass(frozen=True)
class SnowflakeConfig:
    account: str
    user: str
    private_key_path: str
    private_key_passphrase: str | None
    role: str
    warehouse: str
    database: str
    raw_schema: str

    @classmethod
    def from_env(cls) -> SnowflakeConfig:
        missing = [k for k in ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER", "SNOWFLAKE_PRIVATE_KEY_PATH") if not os.getenv(k)]
        if missing:
            raise RuntimeError(f"AQI_TARGET=snowflake but these environment variables are not set: {missing}")
        return cls(
            account=os.environ["SNOWFLAKE_ACCOUNT"],
            user=os.environ["SNOWFLAKE_USER"],
            private_key_path=os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
            private_key_passphrase=os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE") or None,
            role=os.getenv("SNOWFLAKE_ROLE", "AQI_PIPELINE_ROLE"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE", "AQI_WH"),
            database=os.getenv("SNOWFLAKE_DATABASE", "AQI"),
            raw_schema="RAW",
        )


API_URL = "https://air-quality-api.open-meteo.com/v1/air-quality"
TIMEZONE = "Asia/Kolkata"

# Open-Meteo variable name -> our column name
POLLUTANTS: dict[str, str] = {
    "pm2_5": "pm2_5",
    "pm10": "pm10",
    "nitrogen_dioxide": "no2",
    "sulphur_dioxide": "so2",
    "ozone": "o3",
    "carbon_monoxide": "co",
}


@dataclass(frozen=True)
class City:
    city_id: str
    city_name: str
    latitude: float
    longitude: float


def load_cities(path: Path = CITIES_CSV) -> list[City]:
    """Cities live in ONE file (the dbt seed) so extraction and modelling never disagree."""
    with open(path, newline="", encoding="utf-8") as f:
        return [
            City(r["city_id"], r["city_name"], float(r["latitude"]), float(r["longitude"])) for r in csv.DictReader(f)
        ]
