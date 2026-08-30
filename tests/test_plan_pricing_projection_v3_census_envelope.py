from __future__ import annotations

from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SCRIPT = ROOT / "scripts/research/run_plan_pricing_projection_v3_census_envelope.sh"
SOURCE_SHA = "a" * 40
OWNER = "testowner1"
_FAKE_COMMAND = (
    ROOT / "tests/fixtures/plan_pricing_projection_v3_census_envelope_fake.py.txt"
).read_text(encoding="utf-8")
