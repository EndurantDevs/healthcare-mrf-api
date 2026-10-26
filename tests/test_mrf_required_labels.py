# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import initial
from tests.test_mrf_import_lifecycle_contracts import (
    _configure_import_boundary,
    _import_context,
    _import_task,
)

_OMITTED = object()
_INVALID_LABELS = [_OMITTED, None, "", " \t", 0, False, [], {}]
_INVALID_LABEL_IDS = ["omitted", "null", "empty", "blank", "number", "boolean", "array", "object"]


def _label_entry(field, value, **fields):
    if value is not _OMITTED:
        fields[field] = value
    return fields


def _source_plan(**fields):
    return {
        "plan_id": "12345AA0000000",
        "plan_id_type": "CMS-HIOS-PLAN-ID",
        "years": [2026],
        "marketing_name": "Synthetic Plan",
        "summary_url": "https://data.example.invalid/summary",
        "plan_contact": "support@example.invalid",
        "network": [],
        "formulary": [],
        "last_updated_on": "2026-07-01",
        **fields,
    }


def _stage_rows(batches, label):
    return [row for stage, rows in batches if stage == label for row in rows]


def _assert_invalid_label_logged(field):
    assert any(
        call.args[0] == "err" and f"Mandatory field `{field}`" in call.args[1]
        for call in initial.log_error.await_args_list
    )
    initial._mark_mrf_work_done.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("label_value", _INVALID_LABELS, ids=_INVALID_LABEL_IDS)
async def test_plan_network_skips_invalid_labels(monkeypatch, label_value):
    """Retain the parent plan and valid network siblings without fabricated tiers."""
    source = _source_plan(
        network=[
            {"network_tier": "Source-Tier"},
            _label_entry("network_tier", label_value),
            {"network_tier": "SECOND-TIER"},
        ]
    )
    pushed, duplicate_tolerant = _configure_import_boundary(monkeypatch, [source])

    assert await initial.process_plan(_import_context(), _import_task("plans")) == 1

    plans = _stage_rows(pushed + duplicate_tolerant, "Plan")
    assert len(plans) == 1
    assert plans[0]["network"] == ["Source-Tier", "SECOND-TIER"]
    _assert_invalid_label_logged("network_tier")


@pytest.mark.asyncio
@pytest.mark.parametrize("label_value", _INVALID_LABELS, ids=_INVALID_LABEL_IDS)
async def test_formulary_skips_invalid_drug_tiers(monkeypatch, label_value):
    """Reject an unidentifiable formulary while retaining its plan and sibling."""
    source = _source_plan(
        formulary=[
            _label_entry("drug_tier", label_value, mail_order=False, cost_sharing=[{"pharmacy_type": "RETAIL"}]),
            {"drug_tier": "Source-Tier", "mail_order": False, "cost_sharing": [{"pharmacy_type": "RETAIL"}]},
        ]
    )
    pushed, duplicate_tolerant = _configure_import_boundary(monkeypatch, [source])

    assert await initial.process_plan(_import_context(), _import_task("plans")) == 1

    assert len(_stage_rows(pushed + duplicate_tolerant, "Plan")) == 1
    rows = _stage_rows(duplicate_tolerant, "PlanFormulary")
    assert [(row["drug_tier"], row["pharmacy_type"]) for row in rows] == [("Source-Tier", "RETAIL")]
    _assert_invalid_label_logged("drug_tier")


@pytest.mark.asyncio
@pytest.mark.parametrize("label_value", _INVALID_LABELS, ids=_INVALID_LABEL_IDS)
async def test_cost_sharing_skips_invalid_pharmacy_types(monkeypatch, label_value):
    """Keep valid cost-sharing siblings and leave omitted optional amounts null."""
    source = _source_plan(
        formulary=[
            {
                "drug_tier": "SOURCE-TIER",
                "mail_order": False,
                "cost_sharing": [
                    {"pharmacy_type": "Source-Pharmacy", "copay_amount": 0},
                    _label_entry("pharmacy_type", label_value),
                    {"pharmacy_type": "MAIL"},
                ],
            }
        ]
    )
    pushed, duplicate_tolerant = _configure_import_boundary(monkeypatch, [source])

    assert await initial.process_plan(_import_context(), _import_task("plans")) == 1

    assert len(_stage_rows(pushed + duplicate_tolerant, "Plan")) == 1
    rows = _stage_rows(duplicate_tolerant, "PlanFormulary")
    assert [(row["pharmacy_type"], row["copay_amount"]) for row in rows] == [("Source-Pharmacy", 0), ("MAIL", None)]
    assert all(row["drug_tier"] == "SOURCE-TIER" for row in rows)
    _assert_invalid_label_logged("pharmacy_type")


def _provider_source(plans, npi):
    return {
        "npi": npi,
        "type": "INDIVIDUAL",
        "name": {"first": "Synthetic", "last": "Provider"},
        "plans": plans,
        "last_updated_on": "2026-07-01",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("label_value", _INVALID_LABELS, ids=_INVALID_LABEL_IDS)
async def test_provider_network_skips_invalid_links(monkeypatch, label_value):
    """Keep valid links from the same provider and continue to subsequent records."""
    plan_values_by_field = {"plan_id": "12345AA0000000", "years": [datetime.datetime.now().year]}
    valid_plan_by_field = {**plan_values_by_field, "network_tier": "Source-Tier"}
    source_entries = [
        _provider_source(
            [valid_plan_by_field, _label_entry("network_tier", label_value, **plan_values_by_field)], "1000000001"
        ),
        {"npi": "1000000003", "plans": [_label_entry("network_tier", label_value, **plan_values_by_field)]},
        _provider_source([valid_plan_by_field], "1000000002"),
    ]
    _pushed, duplicate_tolerant = _configure_import_boundary(monkeypatch, source_entries)
    monkeypatch.setattr(initial.db, "select", lambda *_args: SimpleNamespace(all=AsyncMock(return_value=[])))
    monkeypatch.setattr(initial, "_push_mrf_address_rows", AsyncMock())
    monkeypatch.setattr(initial, "_build_mrf_address_rows", lambda *_args, **_kwargs: ([], []))
    monkeypatch.setattr(initial, "_mark_mrf_provider_file_progress", AsyncMock())

    assert await initial.process_provider(_import_context(), _import_task("providers")) == 1

    providers = _stage_rows(duplicate_tolerant, "PlanNPIRaw")
    assert {row["npi"] for row in providers} == {1000000001, 1000000002}
    assert {row["network_tier"] for row in providers} == {"Source-Tier"}
    networks = _stage_rows(duplicate_tolerant, "PlanNetworkTierRaw")
    assert len(networks) == 1
    assert networks[0]["network_tier"] == "Source-Tier"
    assert networks[0]["checksum_network"] == initial.return_checksum(
        ["Source-Tier", 12345, datetime.datetime.now().year]
    )
    _assert_invalid_label_logged("network_tier")
