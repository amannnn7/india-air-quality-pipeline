import json
from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


@pytest.fixture
def delhi_payload() -> dict:
    """A real-shaped Open-Meteo response (24 hours, one missing NO2 value)."""
    return json.loads((FIXTURES / "open_meteo_delhi_2025-11-01.json").read_text(encoding="utf-8"))
