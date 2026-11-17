# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Ordinary run admission owns a stable retained-publication attempt."""

from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import registry_network_import as importer
from process.registry_membership_refresh_contract import membership_refresh_command_sha256
from tests.test_registry_imported_membership_refresh import imported_command


def observation():
    command = imported_command()
    return {
        "accepted_sources": command["accepted_sources"],
        "approved_revision": command["expected_approved_revision"],
        "expected_serving_generation": command["expected_serving_generation"],
        "expected_serving_manifest_sha256": command["expected_serving_manifest_sha256"],
        "address_artifact_sha256": command["address_artifact_sha256"],
    }


@pytest.fixture
def prepared_runtime(monkeypatch):
    from process import registry_network_import_runtime as runtime

    freezer = AsyncMock()

    @asynccontextmanager
    async def native_runtime(config, cleanup):
        yield object(), SimpleNamespace(freeze_preparation=freezer)

    monkeypatch.setattr(importer, "_existing_network_import_request", AsyncMock(return_value=None))
    monkeypatch.setattr(runtime, "native_network_import_runtime", native_runtime)
    prepared = AsyncMock()
    prepared.freeze_preparation = freezer
    monkeypatch.setattr(runtime, "prepare_network_import_catalog", prepared)
    return prepared


def test_admitted_run_replay_and_explicit_retry_have_distinct_attempts():
    observed = observation()
    first = importer.latest_network_refresh_envelope(observed, "run-one")
    assert first == importer.latest_network_refresh_envelope(observed, "run-one")
    second = importer.latest_network_refresh_envelope(observed, "run-two")
    assert membership_refresh_command_sha256(first["command"]) != membership_refresh_command_sha256(second["command"])
    assert first["command"]["enrolled_scopes"] == []
    observed["accepted_sources"].clear()
    assert len(first["command"]["accepted_sources"]) == 1


@pytest.mark.parametrize("run_id", [None, "", "one\ntwo", "one two", "x" * 129])
def test_bad_control_run_identity_cannot_create_publication_attempt(run_id):
    with pytest.raises(ValueError):
        importer.network_import_attempt_key(run_id)


@pytest.mark.asyncio
async def test_unchanged_admission_never_starts_publication_worker(monkeypatch, prepared_runtime):
    from process import registry_publication_worker as worker

    unchanged_by_field = {"state": "unchanged", "generation_id": 4, "manifest_sha256": "f" * 64}
    admit = AsyncMock(return_value=(None, unchanged_by_field))
    run = AsyncMock(side_effect=AssertionError("unchanged import must do no composition"))
    monkeypatch.setattr(importer, "admit_latest_network_import", admit)
    monkeypatch.setattr(worker, "run_registry_publication_worker", run)
    assert (
        await importer.run_latest_network_import(SimpleNamespace(operation_seconds=10), "run-one") == unchanged_by_field
    )
    run.assert_not_awaited()


@pytest.mark.asyncio
async def test_new_admission_runs_only_its_exact_request(monkeypatch, prepared_runtime):
    from uuid import UUID

    from process import registry_publication_worker as worker

    identity = UUID("e4e80939-2426-48e0-9894-4b9ca63ac612")
    monkeypatch.setattr(importer, "admit_latest_network_import", AsyncMock(return_value=(identity, None)))
    run = AsyncMock(return_value={"state": "completed"})
    monkeypatch.setattr(worker, "run_registry_publication_worker", run)
    config = SimpleNamespace(operation_seconds=10)
    assert await importer.run_latest_network_import(config, "run-one") == {"state": "completed"}
    run.assert_awaited_once_with(
        config, membership_refresh_request_id=identity, freeze_preparation=prepared_runtime.freeze_preparation
    )


def test_normal_core_parameters_are_empty_and_legacy_exact_selector_stays_valid():
    from process.registry_publication_worker import validate_membership_refresh_worker_params

    assert validate_membership_refresh_worker_params({}) is None
    for invalid in ({"sources": []}, {"operation": "refresh_memberships"}, []):
        with pytest.raises(ValueError):
            validate_membership_refresh_worker_params(deepcopy(invalid))


@pytest.mark.asyncio
@pytest.mark.parametrize("has_replacement", [False, True])
async def test_imported_refresh_preserves_unavailable_predecessor_recipes(monkeypatch, has_replacement):
    from process import registry_imported_source_inventory as inventory_module
    from process import registry_membership_refresh_runtime as runtime
    from process import registry_source_recipe_store as recipe_store
    from tests.test_registry_typed_source_recipe import _recipe

    previous = _recipe()
    native = SimpleNamespace(source_pin=object())
    prepared = object()
    inventory = SimpleNamespace(
        fhir_descriptors=(previous.source_pin.catalog_descriptor,) if has_replacement else (),
        native_sources=(),
    )
    monkeypatch.setattr(inventory_module, "current_imported_source_inventory", AsyncMock(return_value=inventory))
    require_inputs = AsyncMock()
    monkeypatch.setattr(runtime, "require_imported_catalog_inputs", require_inputs)
    monkeypatch.setattr(
        recipe_store, "resolve_retained_registry_source_recipes", AsyncMock(return_value=(native, previous))
    )
    envelope_by_field = {"command": imported_command()}

    @asynccontextmanager
    async def address():
        yield (
            object(),
            object(),
            {"descriptor_sha256": envelope_by_field["command"]["address_artifact_sha256"]},
        )

    resources = SimpleNamespace(address=address, settings=SimpleNamespace(control_schema="registry"))
    resolver = runtime._SourceResolver(resources)
    resolver.prepared_command = membership_refresh_command_sha256(envelope_by_field["command"])
    resolver.prepared_recipes = (prepared,)
    monkeypatch.setattr(resolver, "_composition", lambda pin, recipes, manifest, source_runtime: recipes)
    connection = object()
    async with resolver._pin_imported_sources(connection, envelope_by_field, object()) as recipes:
        assert recipes == (native,) + (() if has_replacement else (previous,)) + (prepared,)
    require_inputs.assert_awaited_once_with(connection, envelope_by_field["command"], "registry", inventory=inventory)


@pytest.mark.asyncio
@pytest.mark.parametrize("state", ["completed", "rejected"])
@pytest.mark.parametrize("entry", ["admit", "run"])
async def test_terminal_request_replay_never_starts_another_preparation(monkeypatch, state, entry):
    from contextlib import AsyncExitStack
    from uuid import uuid4

    from db.models import db
    from process import registry_membership_refresh_queue as queue
    from process import registry_membership_refresh_runtime as membership_runtime

    request_id = uuid4()

    @asynccontextmanager
    async def transaction(**_options):
        yield

    connection = SimpleNamespace(
        transaction=transaction, fetchrow=AsyncMock(return_value={"request_id": request_id, "state": state})
    )

    @asynccontextmanager
    async def acquire():
        yield connection

    validate = Mock()
    monkeypatch.setattr(db, "acquire_driver", acquire)
    monkeypatch.setattr(queue, "validated_membership_refresh_request_status", validate)
    monkeypatch.setattr(
        membership_runtime, "_settings", lambda raw: SimpleNamespace(app_origin=None, control_schema="registry")
    )
    build = AsyncMock(side_effect=AssertionError("terminal replay must not rebuild inputs"))
    monkeypatch.setattr(membership_runtime, "_build_native_bundle", build)
    from process import registry_network_import_runtime as native_runtime

    factory = Mock(side_effect=AssertionError("terminal replay must not open Source"))
    prepare = AsyncMock(side_effect=AssertionError("terminal replay must not prepare metadata"))
    monkeypatch.setattr(native_runtime, "native_network_import_runtime", factory)
    monkeypatch.setattr(native_runtime, "prepare_network_import_catalog", prepare)
    config = SimpleNamespace(operation_seconds=10, consumer=SimpleNamespace(control_schema="registry"))
    async with AsyncExitStack() as cleanup:
        operation = (
            importer.run_latest_network_import(config, "run-one")
            if entry == "run"
            else importer.admit_latest_network_import(config, cleanup, "run-one")
        )
        if state == "rejected":
            with pytest.raises(ValueError, match="publication_rejected"):
                await operation
        else:
            summary_by_field = {"request_id": str(request_id), "state": "completed"}
            assert await operation == (summary_by_field if entry == "run" else (None, summary_by_field))
    validate.assert_called_once_with(connection.fetchrow.return_value)
    build.assert_not_awaited()
    factory.assert_not_called()
    prepare.assert_not_awaited()
