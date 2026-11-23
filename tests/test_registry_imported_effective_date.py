# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""A long ordinary import keeps one policy date while every Source head stays freshly observed."""

from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import registry_fhir_catalog_sync as sync
from process import registry_imported_catalog_preparation as catalog
from process import registry_imported_source_inventory as inventory
from process import registry_imported_source_preparation as preparation
from process.network_fhir_source_witness_contract import OBSERVED_LEGACY_PROFILE, canonical, digest
from process.network_fhir_typed_capture import COMPONENT, validate_typed_capture
from tests.test_registry_imported_source_inventory import inventory_case as inventory_case
from tests.test_registry_imported_source_inventory import observe
from tests.test_registry_imported_source_preparation import prepare, refresh_receipt
from tests.test_registry_imported_source_preparation import prepared_case as prepared_case


@pytest.fixture
def midnight_clock(monkeypatch):
    class Clock(datetime):
        current = datetime(2026, 11, 17, 23, 59, tzinfo=timezone.utc)

        @classmethod
        def now(cls, tz=None):
            return cls.current

    monkeypatch.setattr(inventory, "datetime", Clock)
    monkeypatch.setattr(sync, "datetime", Clock)
    return Clock


@pytest.mark.asyncio
async def test_runtime_pins_policy_date_and_heads_for_its_run(inventory_case, midnight_clock, monkeypatch):
    case = inventory_case

    @asynccontextmanager
    async def reader():
        yield case.connection

    monkeypatch.setattr(inventory, "native_imported_source_reader", reader)
    first_runtime = inventory.ImportedSourceInventoryRuntime(case.config, lambda: None, case.factory)
    with inventory.imported_source_inventory_runtime(first_runtime):
        before = await inventory.current_imported_source_inventory()
        midnight_clock.current = datetime(2026, 11, 18, 0, 5, tzinfo=timezone.utc)
        after = await inventory.current_imported_source_inventory()
        assert after.token == before.token and after.document["effective_date"] == "2026-11-17"
        case.ptg["edition_id"] = "snapshot-two"
        assert await inventory.current_imported_source_inventory() is before
    second_runtime = inventory.ImportedSourceInventoryRuntime(case.config, lambda: None, case.factory)
    with inventory.imported_source_inventory_runtime(second_runtime):
        next_run = await inventory.current_imported_source_inventory()
    assert next_run.document["effective_date"] == "2026-11-18" and next_run.token != before.token
    assert (await observe(case)).token == next_run.token
    assert case.connection.fetchrow.await_count == 3


async def catalogue_inputs(observed, monkeypatch):
    monkeypatch.setattr(
        sync, "_catalog_mapping_inputs", AsyncMock(return_value=(6, {"metadata": "a" * 64, "membership": "b" * 64}))
    )
    token = catalog._INVENTORY.set(observed)
    try:
        return await sync._catalog_snapshot_inputs(object(), SimpleNamespace(sha256="c" * 64), {}, None, None, [])
    finally:
        catalog._INVENTORY.reset(token)


def use_undated_origin(source_case):
    source_case.origin = replace(
        source_case.origin,
        admission_profile=OBSERVED_LEGACY_PROFILE,
        semantic_projection_as_of=None,
        proof_sha256=None,
        admission_metadata_sha256=None,
        descriptor_counts=None,
    )
    source_case.origins[source_case.origin.source_id] = source_case.origin


@pytest.mark.asyncio
async def test_catalogue_and_typed_fallback_share_pinned_date(
    inventory_case, prepared_case, midnight_clock, monkeypatch
):
    source_case = prepared_case
    source_runtime = inventory.ImportedSourceInventoryRuntime(
        inventory_case.config, lambda: None, inventory_case.factory
    )
    observed = await observe(inventory_case)
    use_undated_origin(source_case)
    source_case.connection.fetchval.return_value = datetime(2026, 11, 18, 0, 5, tzinfo=timezone.utc)
    midnight_clock.current = source_case.connection.fetchval.return_value
    with inventory.imported_source_inventory_runtime(source_runtime):
        snapshot, _sources = await catalogue_inputs(observed, monkeypatch)
        first = await prepare(source_case)
        capture = first.recipes[0].source_pin.capture
        assert snapshot["as_of"].isoformat() == capture["graph"]["membership_as_of"] == "2026-11-17"
        assert capture["observed_at"].startswith("2026-11-18T00:05:")
        monkeypatch.setattr(preparation, "predecessor_recipes", AsyncMock(return_value=first.recipes))
        repeated = await prepare(source_case)
    assert repeated.recipes[0] is first.recipes[0]
    assert validate_typed_capture(capture)["graph"]["membership_as_of"] == "2026-11-17"
    changed = deepcopy(capture)
    changed["graph"]["membership_as_of"] = "2026-11-16"
    changed["capture_sha256"] = digest(
        {field: field_value for field, field_value in changed.items() if field != "capture_sha256"}
    )
    with pytest.raises(ValueError, match="membership_date"):
        validate_typed_capture(changed)
    preparation.capture_typed_source_graph.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [False, True], ids=["same-edition", "new-edition"])
async def test_fresh_run_only_reuses_the_prior_date_for_an_exact_edition(inventory_case, prepared_case, changed):
    case = prepared_case
    use_undated_origin(case)
    case.connection.fetchval.return_value = datetime(2026, 11, 17, tzinfo=timezone.utc)
    first = await prepare(case)
    saved = first.recipes[0].source_pin.capture
    metadata = {"metadata": f"{COMPONENT}:{saved['descriptor_version']}:{canonical(saved)}"}
    case.connection.fetchrow.side_effect = lambda query, schema, limit: (
        metadata if schema == saved["schema_name"] else None
    )
    if changed:
        case.descriptors[0]["dataset_sha256"] = "f" * 64
        refresh_receipt(case.descriptors[0])
        case.origins[case.origin.source_id] = replace(case.origin, dataset_sha256="f" * 64)

    today = (await observe(inventory_case)).at_date("2026-11-18")
    observed = replace(today, checkpoint_date="2026-11-17")
    assert observed.token == today.token
    runtime = inventory.ImportedSourceInventoryRuntime(inventory_case.config, lambda: None, inventory_case.factory)
    runtime.observed.append(observed)
    case.connection.fetchval.return_value = datetime(2026, 11, 18, tzinfo=timezone.utc)
    with inventory.imported_source_inventory_runtime(runtime):
        prepared = await prepare(case)

    capture = prepared.recipes[0].source_pin.capture
    if changed:
        assert prepared.created_schemas == (capture["schema_name"],) and not prepared.reused_schemas
        assert capture["graph"]["membership_as_of"] == "2026-11-18" and capture["schema_name"] != saved["schema_name"]
    else:
        assert prepared.reused_schemas == (saved["schema_name"],) and not prepared.created_schemas
        assert capture == saved
