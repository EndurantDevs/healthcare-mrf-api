# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""The extracted fresh-row validator keeps existing shape and special-guard semantics."""

import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

fhir = importlib.import_module("process.provider_directory_fhir")


def _relation(**changes):
    return dict(
        relation_name="synthetic_heap",
        relkind="r",
        relpersistence="p",
        table_am="heap",
        relreplident="d",
        relrowsecurity=False,
        relforcerowsecurity=False,
        relispartition=False,
        user_trigger_count=0,
        rule_count=0,
        inheritance_count=0,
        toast_oid=0,
        **changes,
    )


@pytest.mark.asyncio
async def test_relation_fetch_delegates_exact_observation_and_database(monkeypatch):
    """The original API retains its query, arguments and executing database."""
    relation_row, result = _relation(), ({"validated": True}, None)
    database = SimpleNamespace(first=AsyncMock(return_value=relation_row))
    validator = AsyncMock(return_value=result)
    monkeypatch.setattr(fhir, "_validate_profile_capacity_relation_row", validator)
    assert await fhir._profile_capacity_relation_row(41, "p", 0, database=database) is result
    database.first.assert_awaited_once_with(fhir._PROFILE_CAPACITY_RELATION_SQL, relation_oid=41)
    validator.assert_awaited_once_with(relation_row, "p", 0, database=database)


@pytest.mark.asyncio
@pytest.mark.parametrize("toast_oid", (0, 42))
async def test_fresh_row_validation_returns_same_heap_and_optional_toast_without_fetching(toast_oid):
    """Validation consumes a supplied observation without consulting a different catalog."""
    relation_row = _relation()
    relation_row.update(toast_oid=toast_oid, toast_relkind="t", toast_relpersistence="p", toast_am="heap")
    database = SimpleNamespace(first=AsyncMock(side_effect=AssertionError("validator must not fetch")))
    assert await fhir._validate_profile_capacity_relation_row(relation_row, "p", 0, database=database) == (
        relation_row,
        toast_oid or None,
    )
    database.first.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage",
    (
        None,
        {"relkind": "v"},
        {"relpersistence": "u"},
        {"table_am": "other"},
        {"relreplident": "f"},
        {"relrowsecurity": True},
        {"relforcerowsecurity": True},
        {"relispartition": True},
        {"user_trigger_count": 1},
        {"rule_count": 1},
        {"inheritance_count": 1},
        {"toast_relkind": "r"},
        {"toast_relpersistence": "u"},
        {"toast_am": "other"},
    ),
)
async def test_extracted_validator_keeps_every_heap_and_toast_rejection(damage):
    """Missing, unsafe heap and unsafe TOAST branches retain their original errors."""
    relation_row = None if damage is None else _relation()
    if relation_row is not None:
        relation_row.update(toast_oid=42, toast_relkind="t", toast_relpersistence="p", toast_am="heap")
        relation_row.update(damage)
    with pytest.raises(
        fhir.ProviderDirectoryArtifactBuildStale, match="shape_missing" if damage is None else "shape_unsupported"
    ):
        await fhir._validate_profile_capacity_relation_row(relation_row, "p", 0)


@pytest.mark.asyncio
async def test_extracted_validator_retains_import_run_guard_on_supplied_database(monkeypatch):
    """Import-run trigger custody is checked on the original explicit executor."""
    guards = importlib.import_module("process.provider_directory_import_run_guards")
    guard = AsyncMock()
    monkeypatch.setattr(guards, "assert_import_run_guards", guard)
    monkeypatch.setattr(fhir.profile_initial, "requested", lambda _module: False)
    relation_row = _relation()
    relation_row.update(relation_name=fhir.ImportRun.__tablename__, user_trigger_count=12)
    database = object()
    assert await fhir._validate_profile_capacity_relation_row(relation_row, "p", 0, database=database) == (
        relation_row,
        None,
    )
    guard.assert_awaited_once_with(database, relation_row)


@pytest.mark.asyncio
async def test_extracted_validator_retains_profile_receipt_trigger_guard(monkeypatch):
    """Serving-generation trigger counts still come from the existing native guard."""
    guards = importlib.import_module("process.provider_directory_cms_receipt_guard")
    guard = AsyncMock(return_value=2)
    monkeypatch.setattr(guards, "profile_receipt_guard_count", guard)
    relation_row = _relation()
    relation_row.update(relation_name="provider_directory_profile_serving_generation", user_trigger_count=2)
    database = object()
    assert await fhir._validate_profile_capacity_relation_row(relation_row, "p", 0, database=database) == (
        relation_row,
        None,
    )
    guard.assert_awaited_once_with(database, relation_row)
