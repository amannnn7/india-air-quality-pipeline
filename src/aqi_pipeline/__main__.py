"""Allows `python -m aqi_pipeline ...` as well as the `aqi` command."""

import sys

from .cli import main

sys.exit(main())
