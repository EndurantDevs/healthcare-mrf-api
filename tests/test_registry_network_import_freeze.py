# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Ordinary imports freeze and validate their own prepared data without a second request."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import asdict
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from process import network_membership_publication as publication
from process import registry_network_import_runtime as runtime
from process import registry_publication_worker as worker
from process.network_membership_publication import NetworkPreparationPending
from process.registry_fhir_catalog_custody import FHIRCatalogTarget
from process.registry_imported_catalog_preparation import ImportedCatalogPreparation


@pytest.fixture
def pending_network(monkeypatch):
    copy_target = SimpleNamespace(candidate_id=str(uuid4()))
    handoff_by_field = {
        "kind": "network",
        "candidate_id": copy_target.candidate_id,
        "catalog_schema": "registry",
        "storage_schema": "candidate_data",
    }
    events = []

    @asynccontextmanager
    async def transaction(**_options):
        events.append("read_open")
        try:
            yield
        finally:
            events.append("read_closed")

    connection = SimpleNamespace(transaction=transaction)
    validate = AsyncMock(side_effect=[NetworkPreparationPending(copy_target.candidate_id), {"state": "validated"}])
    load = AsyncMock(return_value=handoff_by_field)
    monkeypatch.setattr(publication, "validate_named_network_preparation", validate)
    monkeypatch.setattr(publication, "load_named_network_preparation", load)
    freezer = AsyncMock(return_value={"preparation_id": copy_target.candidate_id, "state": "frozen"})
    options_by_name = {"control_schema": "registry", "roles": {"publisher_role": "existing_publisher"}}
    return SimpleNamespace(
        target=copy_target,
        handoff=handoff_by_field,
        events=events,
        connection=connection,
        validate=validate,
        load=load,
        freezer=freezer,
        options=options_by_name,
    )


@pytest.mark.asyncio
async def test_network_freezes_and_validates_same_candidate_in_place(pending_network):
    case = pending_network
    assert await worker._seal_import_network_candidate(case.connection, case.target, case.options, case.freezer) == {
        "state": "validated"
    }
    assert case.validate.await_count == 2
    assert all(call.args == (case.connection, case.target) for call in case.validate.await_args_list)
    case.freezer.assert_awaited_once_with(case.handoff, case.options["roles"])
    assert case.events == ["read_open", "read_closed"]


@pytest.mark.asyncio
async def test_legacy_one_shot_preserves_pending_behavior(pending_network):
    case = pending_network
    with pytest.raises(NetworkPreparationPending):
        await worker._seal_import_network_candidate(case.connection, case.target, case.options, None)
    assert case.validate.await_count == 1
    case.load.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [PermissionError("publisher unavailable"), asyncio.CancelledError()])
async def test_failed_or_cancelled_freeze_never_revalidates_or_finishes(pending_network, failure):
    case = pending_network
    case.freezer.side_effect = failure
    with pytest.raises(type(failure)):
        await worker._seal_import_network_candidate(case.connection, case.target, case.options, case.freezer)
    assert case.validate.await_count == 1
    assert case.events == ["read_open", "read_closed"]


@pytest.mark.asyncio
async def test_unfinished_freeze_cannot_return_a_successful_pending_result(pending_network):
    case = pending_network
    case.validate.side_effect = [NetworkPreparationPending(case.target.candidate_id)] * 2
    with pytest.raises(ValueError, match="freeze_incomplete"):
        await worker._seal_import_network_candidate(case.connection, case.target, case.options, case.freezer)


@pytest.mark.asyncio
async def test_freezer_receipt_must_name_the_exact_candidate(pending_network):
    case = pending_network
    case.freezer.return_value["preparation_id"] = str(uuid4())
    with pytest.raises(ValueError, match="freeze_result_invalid"):
        await worker._seal_import_network_candidate(case.connection, case.target, case.options, case.freezer)
    assert case.validate.await_count == 1


def pending_catalog():
    identity = uuid4()
    target = FHIRCatalogTarget(*(str(uuid4()) for _ in range(3)), str(identity), "fhir_catalog_" + identity.hex)
    snapshot_by_field = {
        "schema_name": target.schema_name,
        "metadata_generation": "a" * 64,
        "custody_json": {
            "preparation": {"target": asdict(target), "previous_metadata_generation": None, "accounting": {}}
        },
    }
    return target, ImportedCatalogPreparation(
        "e" * 64, "a" * 64, target.schema_name, snapshot_by_field, None, {"preparation_state": "handed_off"}
    )


@pytest.mark.asyncio
async def test_catalogue_freezes_and_reseals_checkpoint_without_rebuilding(monkeypatch):
    from process import registry_fhir_catalog_store as store
    from process import registry_fhir_catalog_sync as sync

    target, prepared = pending_catalog()
    roles_by_name = {"publisher_role": "existing_publisher"}
    monkeypatch.setattr(store, "named_catalog_preparation_roles", lambda catalog: roles_by_name)
    seal = AsyncMock(return_value="validated")
    monkeypatch.setattr(sync, "_seal_catalog_snapshot", seal)
    freezer = AsyncMock(return_value={"preparation_id": target.candidate_id, "state": "frozen"})
    source_runtime = SimpleNamespace(freeze_preparation=freezer, catalog_export=object())
    catalog = SimpleNamespace(consumer=SimpleNamespace(control_schema="registry"))
    sealer_pool = object()
    finished = await runtime._finish_import_catalog(catalog, source_runtime, prepared, sealer_pool)
    assert finished.snapshot is prepared.snapshot and finished.metadata_generation == prepared.metadata_generation
    assert finished.accounting["preparation_state"] == "validated"
    freezer.assert_awaited_once_with(store.named_catalog_preparation(prepared.snapshot, "registry"), roles_by_name)
    seal.assert_awaited_once_with(catalog, target, prepared.snapshot, sealer_pool)


@pytest.mark.asyncio
async def test_ordinary_run_keeps_one_owner_request_and_preparation_until_complete(pending_network, monkeypatch):
    from process import registry_network_import as importer

    case = pending_network
    events = []
    catalog = SimpleNamespace(consumer=SimpleNamespace(control_schema="registry"))
    source_runtime = SimpleNamespace(freeze_preparation=case.freezer)

    @asynccontextmanager
    async def owner(_config, _cleanup):
        events.append("source_open")
        try:
            yield catalog, source_runtime
        finally:
            events.append("source_closed")

    prepare = AsyncMock()
    request_id = uuid4()
    admit = AsyncMock(return_value=(request_id, None))

    async def publish(config, *, membership_refresh_request_id, freeze_preparation):
        assert events == ["source_open"] and membership_refresh_request_id == request_id
        publication_result = await worker._seal_import_network_candidate(
            case.connection, case.target, case.options, freeze_preparation
        )
        assert publication_result == {"state": "validated"}
        return {"state": "completed", "request_id": str(request_id)}

    monkeypatch.setattr(importer, "_existing_network_import_request", AsyncMock(return_value=None))
    monkeypatch.setattr(runtime, "native_network_import_runtime", owner)
    monkeypatch.setattr(runtime, "prepare_network_import_catalog", prepare)
    monkeypatch.setattr(importer, "admit_latest_network_import", admit)
    monkeypatch.setattr(worker, "run_registry_publication_worker", publish)
    publication_result = await importer.run_latest_network_import(SimpleNamespace(operation_seconds=10), "run-one")
    assert publication_result == {"state": "completed", "request_id": str(request_id)}
    assert events == ["source_open", "source_closed"]
    assert prepare.await_count == admit.await_count == case.freezer.await_count == 1
    assert case.validate.await_count == 2


@pytest.mark.asyncio
async def test_freeze_retries_lock_timeouts_and_keeps_validation_errors(monkeypatch):
    import asyncpg
    from sqlalchemy.exc import DBAPIError

    handoff_by_field = {"kind": "network", "candidate_id": str(uuid4())}
    receipt_by_field = {"preparation_id": handoff_by_field["candidate_id"], "state": "frozen"}
    freezer = AsyncMock(side_effect=[asyncpg.LockNotAvailableError("busy"), TimeoutError(), receipt_by_field])
    sleep = AsyncMock()
    monkeypatch.setattr(runtime.asyncio, "sleep", sleep)
    assert await runtime.freeze_network_import_preparation(freezer, handoff_by_field, {}) == receipt_by_field
    assert freezer.await_count == 3 and sleep.await_count == 2
    wrapped = DBAPIError("SET LOCAL lock_timeout", {}, asyncpg.LockNotAvailableError("busy"))
    freezer = AsyncMock(side_effect=[wrapped, receipt_by_field])
    assert await runtime.freeze_network_import_preparation(freezer, handoff_by_field, {}) == receipt_by_field
    for permanent in (ValueError("preparation_not_handed_off"), asyncpg.InsufficientPrivilegeError("denied")):
        refused = AsyncMock(side_effect=permanent)
        with pytest.raises(type(permanent)):
            await runtime.freeze_network_import_preparation(refused, handoff_by_field, {})
        assert refused.await_count == 1
