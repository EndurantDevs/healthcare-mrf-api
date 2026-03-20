# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import json
import types
from datetime import datetime
from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path

import pytest

MODULE_PATH = Path(__file__).resolve().parents[1] / "api" / "endpoint" / "coverage.py"
MODULE_SPEC = spec_from_file_location("coverage_endpoint_unit", MODULE_PATH)
coverage_module = module_from_spec(MODULE_SPEC)
assert MODULE_SPEC and MODULE_SPEC.loader
MODULE_SPEC.loader.exec_module(coverage_module)

coverage_statistics = coverage_module.coverage_statistics


class FakeResult:
    def __init__(self, scalar=None):
        self._scalar = scalar

    def scalar(self):
        return self._scalar


class FakeSession:
    def __init__(self, results=None):
        self._results = list(results or [])

    async def execute(self, *_args, **_kwargs):
        if not self._results:
            return FakeResult(0)
        return self._results.pop(0)


def make_request(results=None):
    session = FakeSession(results)
    return types.SimpleNamespace(ctx=types.SimpleNamespace(sa_session=session))


@pytest.mark.asyncio
async def test_coverage_statistics_success(monkeypatch):
    async def _all_tables_exist(_session, _table):
        return True

    monkeypatch.setattr(coverage_module, "_table_exists", _all_tables_exist)

    request = make_request(
        [
            FakeResult(datetime(2026, 3, 5, 11, 58, 22)),
            FakeResult(14311),
            FakeResult(735),
            FakeResult(1259245),
            FakeResult(20884),
            FakeResult(1175281),
            FakeResult(9297),
            FakeResult(8738),
            FakeResult(1158735),
            FakeResult(7441),
            FakeResult(3214),
            FakeResult(12692040),
            FakeResult(1259245),
            FakeResult(1104162),
            FakeResult(3144),
        ]
    )

    response = await coverage_statistics(request)
    payload = json.loads(response.body)

    assert payload == {
        "snapshot_date": "2026-03-05T11:58:22",
        "marketplace_plans": 14311,
        "marketplace_issuers": 735,
        "provider_coverage_profiles": 1259245,
        "medicare_clinicians": 1158735,
        "hospitals_with_medicare_enrollment": 8738,
        "providers_with_procedure_history": 1175281,
        "procedure_codes_tracked": 9297,
        "zip_codes_with_procedure_coverage": 20884,
        "active_medicare_pharmacies": 7441,
        "formularies_tracked": 3214,
        "drug_coverage_rows": 12692040,
        "providers_with_quality_scores": 1259245,
        "providers_with_prescription_history": 1104162,
        "prescription_codes_tracked": 3144,
    }


@pytest.mark.asyncio
async def test_coverage_statistics_missing_tables_return_zero(monkeypatch):
    async def _no_tables_exist(_session, _table):
        return False

    monkeypatch.setattr(coverage_module, "_table_exists", _no_tables_exist)

    request = make_request([])
    response = await coverage_statistics(request)
    payload = json.loads(response.body)

    int_fields = {
        "marketplace_plans",
        "marketplace_issuers",
        "provider_coverage_profiles",
        "medicare_clinicians",
        "hospitals_with_medicare_enrollment",
        "providers_with_procedure_history",
        "procedure_codes_tracked",
        "zip_codes_with_procedure_coverage",
        "active_medicare_pharmacies",
        "formularies_tracked",
        "drug_coverage_rows",
        "providers_with_quality_scores",
        "providers_with_prescription_history",
        "prescription_codes_tracked",
    }

    assert isinstance(payload.get("snapshot_date"), str)
    assert payload["snapshot_date"]
    for field in int_fields:
        assert payload[field] == 0
        assert isinstance(payload[field], int)
