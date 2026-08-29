# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import initial
from tests.test_mrf_import_lifecycle_contracts import (
    _configure_import_boundary,
    _import_context,
    _import_task,
)


@pytest.mark.asyncio
async def test_plan_import_flushes_multi_plan_buffers_without_losing_rows(monkeypatch):
    """Flush retained rows across ordinary one-year plans at the row threshold."""
    plan_ids = [f"12345AA000000{suffix}" for suffix in range(3)]
    plan_source_entries = [
        {
            "plan_id": plan_id,
            "plan_id_type": "CMS-HIOS-PLAN-ID",
            "years": [2026],
            "marketing_name": f"Synthetic Plan {index + 1}",
            "summary_url": f"https://data.example.invalid/summary/{index + 1}",
            "plan_contact": "support@example.invalid",
            "network": [],
            "formulary": {
                "drug_tier": "GENERIC",
                "mail_order": False,
                "cost_sharing": [
                    {
                        "pharmacy_type": "RETAIL",
                        "copay_amount": index + 1,
                        "copay_opt": "NONE",
                        "coinsurance_rate": 0,
                        "coinsurance_opt": "NONE",
                    }
                ],
            },
            "benefits": [],
            "last_updated_on": "2026-07-01",
        }
        for index, plan_id in enumerate(plan_ids)
    ]
    pushed_batches, duplicate_tolerant_batches = _configure_import_boundary(
        monkeypatch, plan_source_entries
    )
    monkeypatch.setattr(initial, "_mrf_plan_flush_rows", lambda _test_mode: 2)

    outcome = await initial.process_plan(_import_context(), _import_task("plans"))

    assert outcome == 1
    plan_batches = [
        batch_entries
        for stage_label, batch_entries in pushed_batches + duplicate_tolerant_batches
        if stage_label == "Plan" and batch_entries
    ]
    formulary_batches = [
        batch_entries
        for stage_label, batch_entries in duplicate_tolerant_batches
        if stage_label == "PlanFormulary" and batch_entries
    ]
    assert [len(batch_entries) for batch_entries in plan_batches] == [2, 1]
    assert [len(batch_entries) for batch_entries in formulary_batches] == [2, 1]
    assert {
        plan_entry["plan_id"]
        for batch_entries in plan_batches
        for plan_entry in batch_entries
    } == set(plan_ids)
    assert {
        formulary_entry["plan_id"]
        for batch_entries in formulary_batches
        for formulary_entry in batch_entries
    } == set(plan_ids)


@pytest.mark.asyncio
async def test_provider_import_keeps_each_plans_years_isolated(monkeypatch):
    """Do not leak one provider plan's years into another plan."""
    current_year = datetime.datetime.now().year
    provider_source_entries = [
        {
            "npi": "1000000004",
            "type": "INDIVIDUAL",
            "name": {"first": "Year", "last": "Boundary"},
            "addresses": [],
            "plans": [
                {
                    "plan_id": "12345AA0000000",
                    "network_tier": "CURRENT",
                    "years": [current_year],
                },
                {
                    "plan_id": "23456AA0000000",
                    "network_tier": "NEXT",
                    "years": [current_year + 1],
                },
            ],
            "last_updated_on": "2026-07-01",
        }
    ]
    _pushed_batches, duplicate_tolerant_batches = _configure_import_boundary(
        monkeypatch, provider_source_entries
    )
    monkeypatch.setattr(initial, "_mrf_provider_flush_rows", lambda _test_mode: 100)
    monkeypatch.setattr(
        initial.db,
        "select",
        lambda *_args: SimpleNamespace(all=AsyncMock(return_value=[])),
    )
    monkeypatch.setattr(initial, "_build_mrf_address_rows", lambda *_args, **_kwargs: ([], []))
    monkeypatch.setattr(initial, "_push_mrf_address_rows", AsyncMock())
    monkeypatch.setattr(initial, "_mark_mrf_provider_file_progress", AsyncMock())

    outcome = await initial.process_provider(
        _import_context(), _import_task("providers")
    )

    assert outcome == 1
    network_entries = [
        network_entry
        for stage_label, batch_entries in duplicate_tolerant_batches
        if stage_label == "PlanNetworkTierRaw"
        for network_entry in batch_entries
    ]
    assert {
        (entry["plan_id"], entry["network_tier"], entry["year"])
        for entry in network_entries
    } == {
        ("12345AA0000000", "CURRENT", current_year),
        ("23456AA0000000", "NEXT", current_year + 1),
    }


@pytest.mark.asyncio
async def test_provider_import_reuses_issuer_names_within_one_worker(monkeypatch):
    """Load issuer display names only once for one worker context."""
    _configure_import_boundary(monkeypatch, [])
    issuer_query = Mock(
        return_value=SimpleNamespace(
            all=AsyncMock(
                return_value=[
                    SimpleNamespace(
                        issuer_id=12345,
                        issuer_name="Synthetic Issuer",
                        issuer_marketing_name="",
                        mrf_url="https://synthetic.example/index.json",
                    )
                ]
            )
        )
    )
    monkeypatch.setattr(initial.db, "select", issuer_query)
    monkeypatch.setattr(initial, "_mark_mrf_provider_file_progress", AsyncMock())
    context = _import_context()

    assert await initial.process_provider(context, _import_task("providers")) == 1
    assert await initial.process_provider(context, _import_task("providers")) == 1

    assert issuer_query.call_count == 1
    assert next(iter(context["mrf_issuer_lookup"].values())) == {
        12345: "Synthetic Issuer"
    }
