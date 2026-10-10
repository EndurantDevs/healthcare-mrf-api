"""Closed published command, complete files and fresh authority regression."""

import copy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import registry_ptg_published_plan_contract as contract
from process import registry_ptg_published_plan_scope as review
from process import registry_ptg_published_plan_source as source
from process import registry_ptg_scope_engine as engine
from process.registry_ptg_producer_scope import RegistryPTGProducerScopeError
from tests.test_registry_ptg_scope_engine import _intent, _ownership


def intent():
    command = _intent()
    del command["company_key"]
    del command["cohort_id"]
    return {
        **command,
        "review_type": contract.REVIEW_TYPE,
        "selection_mode": contract.SELECTION_MODE,
        "network_id": 7,
        "plan_id": "plan_example",
        "plan_market_type": "group",
    }


def identity():
    return {
        "snapshot_id": "snapshot_example",
        "import_run_id": "run_example",
        "import_month": "opaque-period",
        "source_key": "source_example",
        "snapshot_key": 17,
        "source_count": 1,
        "plan_id": "plan_example",
        "plan_market_type": "group",
        **{name: "c" * 64 for name in contract.DIGEST_FIELDS},
    }


@pytest.mark.parametrize(
    "mutation",
    [
        {"company_key": "invented"},
        {"network_id": True},
        {"network_id": 0},
        {"network_id": 2147483648},
        {"selection_mode": "plan_specific_subset"},
        {"plan_market_type": "GROUP"},
        {"review_type": "frozen"},
    ],
)
def test_closed_published_intent_refuses_inference_and_invalid_selection(mutation):
    with pytest.raises(ValueError):
        engine._intent(intent() | mutation)


@pytest.mark.asyncio
async def test_resolver_uses_actual_plan_and_complete_files(monkeypatch):
    privileges = AsyncMock()
    monkeypatch.setattr(review, "require_published_lock_privileges", privileges)
    inventory_by_field = {
        "identity": identity(),
        "authority": {"contract": contract.AUTHORITY_CONTRACT},
        "plan_scopes": [{"plan_id": "plan_example", "plan_market_type": "group"}],
        "file_versions": intent()["file_versions"],
        "source_keys": [1],
    }
    prepared = AsyncMock(return_value=inventory_by_field)
    monkeypatch.setattr(review, "published_plan_inventory", prepared)
    full, authority, evidence = await review.resolve_published_plan(None, "synthetic_ptg", intent(), _ownership(), {})
    privileges.assert_awaited_once_with(None, "synthetic_ptg", pin_writer=False)
    assert engine._full(full) == full
    assert "company_key" not in full and "cohort_id" not in full
    assert evidence["selected_source_keys"] == [1]
    assert authority["contract"] == contract.AUTHORITY_CONTRACT
    for changed in [intent() | {"plan_id": "other_plan"}, intent() | {"file_versions": []}]:
        with pytest.raises((ValueError, RegistryPTGProducerScopeError)):
            await review.resolve_published_plan(None, "synthetic_ptg", changed, _ownership(), {})


@pytest.mark.asyncio
async def test_full_command_refuses_cross_snapshot_and_file_omission(monkeypatch):
    privileges = AsyncMock()
    monkeypatch.setattr(review, "require_published_lock_privileges", privileges)
    monkeypatch.setattr(
        review,
        "published_plan_inventory",
        AsyncMock(
            return_value={
                "identity": identity(),
                "authority": {"contract": contract.AUTHORITY_CONTRACT},
                "plan_scopes": [{"plan_id": "plan_example", "plan_market_type": "group"}],
                "file_versions": intent()["file_versions"],
                "source_keys": [1],
            }
        ),
    )
    full, _, _ = await review.resolve_published_plan(None, "synthetic_ptg", intent(), _ownership(), {})
    privileges.assert_awaited_once_with(None, "synthetic_ptg", pin_writer=False)
    for field, value in [("snapshot_id", "other_snapshot"), ("source_count", 2)]:
        changed = copy.deepcopy(full)
        changed["published_identity"][field] = value
        with pytest.raises(ValueError):
            contract.validated_command(changed)


@pytest.mark.asyncio
async def test_resolver_refuses_unsafe_privileges_before_source_inventory(monkeypatch):
    inventory = AsyncMock()
    monkeypatch.setattr(review, "published_plan_inventory", inventory)
    monkeypatch.setattr(
        review,
        "require_published_lock_privileges",
        AsyncMock(side_effect=RegistryPTGProducerScopeError("registry_ptg_published_lock_privileges_unprotected")),
    )
    with pytest.raises(RegistryPTGProducerScopeError, match="registry_ptg_published_lock_privileges_unprotected"):
        await review.resolve_published_plan(None, "synthetic_ptg", intent(), _ownership(), {})
    inventory.assert_not_awaited()


@pytest.mark.asyncio
async def test_file_inventory_refuses_missing_raw_trace_witness():
    assigned = SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: [{"source_key": 1}]))
    missing = SimpleNamespace(scalar_one=lambda: True)
    session = SimpleNamespace(execute=AsyncMock(side_effect=[assigned, missing]))
    monkey = pytest.MonkeyPatch()
    monkey.setattr(source, "_digest", lambda _: "c" * 64)
    try:
        with pytest.raises(RegistryPTGProducerScopeError):
            await source._published_file_rows(
                session,
                '"synthetic_ptg"',
                "snapshot_example",
                {"source_count": 1, "source_assignments_sha256": "c" * 64},
            )
        assert session.execute.await_count == 2
    finally:
        monkey.undo()
