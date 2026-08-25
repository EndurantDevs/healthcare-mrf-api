# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path

import pytest


class _FakeDb:
    rows = []

    async def all(self, statement):
        assert "hospital_price_current" in statement
        return self.rows


_registry = (
    {"hospital_id": "hospital-000001", "name": "Alpha", "cms_hpt_url": "https://a.example/cms-hpt.txt"},
    {"hospital_id": "hospital-000002", "name": "Beta", "cms_hpt_url": "https://b.example/cms-hpt.txt"},
    {"hospital_id": "hospital-000003", "name": "Gamma", "cms_hpt_url": "https://c.example/cms-hpt.txt"},
)


def _load_module():
    fake_db_models = types.ModuleType("db.models")
    fake_db_models.db = _FakeDb()
    fake_registry = types.ModuleType("process.hospital_hpt_registry")
    fake_registry.load_hospital_hpt_registry = lambda: _registry
    replacement_by_module = {
        "db.models": fake_db_models,
        "process.hospital_hpt_registry": fake_registry,
    }
    previous_by_module = {
        name: sys.modules.get(name) for name in replacement_by_module
    }
    sys.modules.update(replacement_by_module)
    try:
        path = Path(__file__).parents[1] / "api/hospital_price_status.py"
        spec = importlib.util.spec_from_file_location(
            "hospital_price_status_isolated", path
        )
        assert spec is not None and spec.loader is not None
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module
    finally:
        for name, old_module in previous_by_module.items():
            if old_module is None:
                sys.modules.pop(name, None)
            else:
                sys.modules[name] = old_module


status_api = _load_module()


@pytest.mark.asyncio
async def test_page_keeps_latest_attempt_separate_from_last_good_publication():
    status_api.db.rows = [
        {
            "hospital_id": "hospital-000001",
            "facility_anchor_id": "facility-1",
            "attempt_id": "attempt-1",
            "attempt_status": "failed",
            "error_code": "source_unavailable",
            "version_id": "a" * 64,
            "generation": 2,
            "service_count": 10,
            "charge_count": 20,
            "payer_charge_count": 30,
            "npi_count": 2,
            "tax_identity_count": 1,
        },
        {
            "hospital_id": "hospital-000002",
            "attempt_id": "attempt-2",
            "attempt_status": "running",
        },
    ]

    page = await status_api.list_hospital_price_status_page(limit=1)

    assert page["items"][0]["latest_attempt"]["status"] == "failed"
    assert page["items"][0]["publication"]["generation"] == 2
    assert page["next_cursor"] == "hospital-000001"
    assert page["summary"] == {
        "total": 3,
        "queued": 0,
        "running": 1,
        "succeeded": 1,
        "failed": 1,
        "unpublished": 2,
    }


@pytest.mark.asyncio
async def test_query_status_and_cursor_are_stable():
    status_api.db.rows = []

    page = await status_api.list_hospital_price_status_page(
        query="example", status="unpublished", cursor="hospital-000001", limit=10
    )

    assert [item["hospital_id"] for item in page["items"]] == [
        "hospital-000002",
        "hospital-000003",
    ]
    assert page["next_cursor"] is None


@pytest.mark.parametrize(
    "kwargs",
    [
        {"limit": 0},
        {"cursor": "bad"},
        {"status": "unknown"},
    ],
)
@pytest.mark.asyncio
async def test_invalid_page_inputs_fail_closed(kwargs):
    status_api.db.rows = []

    with pytest.raises(ValueError):
        await status_api.list_hospital_price_status_page(**kwargs)
