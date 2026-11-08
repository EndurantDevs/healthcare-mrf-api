# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Standalone contact repair requires real snapshot authority before any write."""

import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_output_contact_backfill as contacts
from process import provider_directory_output_location_rebuild as rebuild

fhir = importlib.import_module("process.provider_directory_fhir")


@pytest.mark.asyncio
async def test_standalone_contact_dispatch_preserves_locator_and_never_updates_live_rows(monkeypatch):
    staged = AsyncMock(return_value={"location_contact_rows_updated": 7})
    monkeypatch.setattr(contacts, "backfill_location_contacts", staged)
    monkeypatch.setattr(fhir.db, "status", AsyncMock(side_effect=AssertionError("live mutation reached")))
    monkeypatch.setattr(
        fhir, "_ensure_provider_directory_tables", AsyncMock(side_effect=AssertionError("bootstrap reached"))
    )
    token = fhir._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.set({})
    try:
        assert await fhir.backfill_provider_directory_location_contacts(run_id="maintenance-a") == {
            "location_contact_rows_updated": 7
        }
    finally:
        fhir._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.reset(token)
    staged.assert_awaited_once_with(fhir, run_id="maintenance-a")


@pytest.mark.asyncio
@pytest.mark.parametrize("run_id", [False, 0, ""])
async def test_invalid_run_locator_refuses_before_any_transaction(monkeypatch, run_id):
    monkeypatch.setattr(rebuild, "source_transaction", lambda *_args, **_kwargs: pytest.fail("database reached"))
    with pytest.raises(RuntimeError, match="contact_run_invalid"):
        await rebuild._captured_operation(SimpleNamespace(), run_id)


@pytest.mark.asyncio
@pytest.mark.parametrize("reader_bound", [False, True])
async def test_original_outer_writer_or_reader_cannot_become_whole_repair_transaction(monkeypatch, reader_bound):
    database = SimpleNamespace(
        _transaction_binding=lambda: None if reader_bound else object(), has_reader_session=lambda: reader_bound
    )
    monkeypatch.setattr(rebuild, "source_transaction", lambda *_args, **_kwargs: pytest.fail("database reached"))
    with pytest.raises(RuntimeError, match="contact_unbound_operation_required"):
        await rebuild._captured_operation(SimpleNamespace(db=database), "maintenance-a")


def test_projection_uses_fixed_native_expressions_and_preserves_complete_output_shape():
    projection, count = contacts.contact_projection(fhir)
    assert "UPDATE " not in projection and "INSERT " not in projection
    assert "repair_eligible" in projection and "repair_changed" in projection
    assert "CAST(:update_timestamp AS timestamp)" in projection
    for field in ("source_id", "resource_id", "fhir_meta", "latitude", "observed_at"):
        assert f'c."{field}" AS "{field}"' in projection
    assert "FILTER(WHERE repair_changed)" in count


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [0, 4])
async def test_issued_loading_result_avoids_standalone_cut(monkeypatch, count):
    from process import provider_directory_artifact_scratch_inputs as scratch

    expected_by_field = {"location_contact_rows_updated": count}
    issued = AsyncMock(return_value=expected_by_field)
    fallback = AsyncMock(side_effect=AssertionError("standalone cut reached"))
    monkeypatch.setattr(scratch, "prepared_scratch_enrichment", issued)
    monkeypatch.setattr(rebuild, "rebuild_location_snapshot", fallback)
    assert await contacts.backfill_location_contacts(fhir) == expected_by_field
    issued.assert_awaited_once_with(fhir, "contact", run_id=None)
    fallback.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("issued", [False, True])
async def test_scoped_contact_dispatch_reuses_only_authenticated_prepared_metrics(monkeypatch, issued):
    from contextlib import asynccontextmanager

    from process import provider_directory_artifact_scratch_inputs as scratch
    from process import provider_directory_cms_preparation as preparation

    @asynccontextmanager
    async def transaction(_directory):
        yield

    prepared = AsyncMock(return_value={"location_contact_rows_updated": 4})
    bootstrap = AsyncMock()
    update = AsyncMock(return_value="UPDATE 7")
    monkeypatch.setattr(scratch, "active_scratch_inputs", lambda directory: object() if issued else None)
    monkeypatch.setattr(scratch, "prepared_scratch_enrichment", prepared)
    monkeypatch.setattr(preparation, "active_nonprofile_sql_transaction", transaction)
    monkeypatch.setattr(fhir, "_ensure_provider_directory_tables", bootstrap)
    monkeypatch.setattr(fhir.db, "status", update)
    token = fhir._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.set({contacts.TARGET: "staged_location"})
    try:
        assert await fhir.backfill_provider_directory_location_contacts() == {
            "location_contact_rows_updated": 4 if issued else 7
        }
    finally:
        fhir._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.reset(token)
    if issued:
        prepared.assert_awaited_once_with(fhir, "contact", run_id=None)
        bootstrap.assert_not_awaited()
        update.assert_not_awaited()
    else:
        prepared.assert_not_awaited()
        bootstrap.assert_awaited_once_with()
        update.assert_awaited_once()
