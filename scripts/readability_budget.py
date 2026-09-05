#!/usr/bin/env python3
"""Report readability debt and block new function-level and architecture debt."""

from __future__ import annotations

import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

from readability.cli import build_snapshot, collect_issues, main, parse_args

__all__ = ["build_snapshot", "collect_issues", "main", "parse_args"]


if __name__ == "__main__":
    raise SystemExit(main())
