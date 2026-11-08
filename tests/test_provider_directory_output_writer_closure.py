# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Unscoped intake cannot reach the former live compatibility writer."""

import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

fhir = importlib.import_module("process.provider_directory_fhir")


@pytest.mark.asyncio
async def test_nonempty_unscoped_intake_refuses_before_preparation_or_any_database_io(monkeypatch):
    prepare = Mock(side_effect=AssertionError("semantic preparation reached"))
    persist = AsyncMock(side_effect=AssertionError("database writer reached"))
    monkeypatch.setattr(fhir, "_prepared_resource_write_rows", prepare)
    monkeypatch.setattr(fhir, "_persist_compatible_resource_rows", persist)
    with pytest.raises(RuntimeError, match="dataset_scope_required"):
        await fhir._upsert_resource_rows(
            fhir.ProviderDirectoryLocation,
            [{"source_id": "source_a", "resource_id": "location_a"}],
            options=fhir.ProviderDirectoryResourceWriteOptions(run_id=None, track_seen=False),
        )
    prepare.assert_not_called()
    persist.assert_not_awaited()


@pytest.mark.asyncio
async def test_empty_legacy_intake_keeps_its_existing_noop_without_managed_reads():
    assert (
        await fhir._upsert_resource_rows(
            fhir.ProviderDirectoryLocation,
            [],
            options=fhir.ProviderDirectoryResourceWriteOptions(run_id=None, track_seen=False),
        )
        == 0
    )


@pytest.mark.asyncio
async def test_isolated_source_sink_preserves_deduplicated_count_without_live_side_effects(monkeypatch):
    persist = AsyncMock(return_value=[{"resource_id": "location_a"}])
    live = AsyncMock(side_effect=AssertionError("live compatibility writer reached"))
    monkeypatch.setattr(fhir, "_persist_endpoint_dataset_rows", persist)
    monkeypatch.setattr(fhir, "_upsert_canonical_resource_edges", live)
    monkeypatch.setattr(fhir, "_mark_resource_rows_seen", live)
    monkeypatch.setattr(fhir, "_upsert_rows", live)
    identity = SimpleNamespace(
        dataset_id="dataset_a", resource_hash_contract="contract_a", semantic_projection_as_of=None
    )
    result = await fhir._persist_compatible_resource_rows(
        fhir.ProviderDirectoryLocation,
        [{"resource_id": "location_a"}],
        fhir.ProviderDirectoryResourceWriteOptions(run_id=None, track_seen=False),
        identity,
    )
    assert result == 1
    live.assert_not_awaited()
    assert persist.await_args.kwargs["resource_hash_contract"] == "contract_a"


@pytest.mark.asyncio
@pytest.mark.parametrize("entrypoint", ["_upsert_rows_values", "_copy_upsert_rows", "_upsert_normalized_rows"])
async def test_managed_live_output_has_no_generic_copy_or_values_fallback(entrypoint):
    call = getattr(fhir, entrypoint)
    with pytest.raises(RuntimeError, match="isolated_copy_required"):
        await call(
            fhir.ProviderDirectoryLocation,
            [{"source_id": "source_a", "resource_id": "location_a"}],
            ["source_id", "resource_id"],
            ["source_id", "resource_id"],
            skip_unchanged=False,
        )
