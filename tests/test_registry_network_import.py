# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Ordinary run admission owns a stable retained-publication attempt."""

import json
from contextlib import AsyncExitStack, asynccontextmanager, nullcontext
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
@pytest.mark.parametrize("caller_snapshot", [False, True])
async def test_imported_refresh_preserves_unavailable_predecessor_recipes(
    monkeypatch, has_replacement, caller_snapshot
):
    from process import registry_imported_source_inventory as inventory_module
    from process import registry_membership_refresh_runtime as runtime
    from process import registry_source_recipe_store as recipe_store
    from tests.test_registry_typed_source_recipe import _recipe

    previous = _recipe()
    native = SimpleNamespace(source_pin=object())
    prepared = object()
    inventory = SimpleNamespace(
        fhir_descriptors=(previous.source_pin.catalog_descriptor,) if has_replacement else (),
        carried_fhir_descriptors=(),
        superseded_fhir_endpoints=(),
        native_sources=(),
    )
    monkeypatch.setattr(inventory_module, "current_imported_source_inventory", AsyncMock(return_value=inventory))
    require_inputs = AsyncMock()
    monkeypatch.setattr(runtime, "require_imported_catalog_inputs", require_inputs)
    envelope_by_field = {"command": imported_command()}
    state = SimpleNamespace(has_snapshot=caller_snapshot)

    @asynccontextmanager
    async def transaction(*, isolation, readonly):
        assert not state.has_snapshot and isolation == "repeatable_read" and readonly is True
        state.has_snapshot = True
        try:
            yield
        finally:
            state.has_snapshot = False

    async def retained_recipes(connection, manifest, *, control_schema):
        assert connection.is_in_transaction()
        return native, previous

    monkeypatch.setattr(recipe_store, "resolve_retained_registry_source_recipes", retained_recipes)

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
    connection = SimpleNamespace(is_in_transaction=lambda: state.has_snapshot, transaction=transaction)
    async with resolver._pin_imported_sources(connection, envelope_by_field, object()) as recipes:
        assert recipes == (native,) + (() if has_replacement else (previous,)) + (prepared,)
        assert state.has_snapshot == caller_snapshot
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


@pytest.fixture
def observed_source_inventory(monkeypatch):
    from process import registry_imported_membership_refresh as admission
    from process import registry_imported_source_inventory as inventory
    from tests.test_registry_imported_source_inventory import native_aca_source, native_ptg_source

    complete = inventory.ImportedSourceInventory(
        json.dumps(
            {
                "contract": "registry-imported-catalog-inputs-v1",
                "fhir_semantics": [],
                "fhir_descriptors": [],
                "native_sources": [native_aca_source(), native_ptg_source()],
                "unavailable": [],
                "effective_date": "2026-01-01",
                "search_policy": "native-network-search-v1",
                "catalog_projector_version": inventory.PROJECTOR_VERSION,
            }
        )
    )
    observe = AsyncMock(return_value=complete)
    monkeypatch.setattr(inventory, "current_imported_source_inventory", observe)
    monkeypatch.setattr(admission, "current_imported_source_inventory", observe)
    return complete


def _queued_refresh_record(observed, run_id):
    from uuid import uuid4

    from process.registry_publication_queue import publication_request_metadata

    envelope = importer.latest_network_refresh_envelope(observed, run_id)
    command_by_field = dict(envelope["command"])
    key = command_by_field.pop("idempotency_key")
    return {
        **publication_request_metadata(command_by_field, envelope["actor"], None, key),
        "request_id": uuid4(),
        "idempotency_key": key,
        "session_token_sha256": None,
    }


@pytest.fixture
def native_import_admission(monkeypatch):
    from db.models import db
    from process import registry_membership_refresh_queue as queue
    from process import registry_membership_refresh_runtime as runtime
    from process.registry_imported_membership_refresh import RegistryImportedMembershipAdmission

    settings = SimpleNamespace(app_origin=None, control_schema="registry")
    config = SimpleNamespace(consumer=SimpleNamespace(control_schema=settings.control_schema))
    observed = observation() | {"address_generation": "a" * 64}
    observed["published_inputs_digest"] = runtime._semantic_digest(
        observed["accepted_sources"],
        observed["approved_revision"],
        observed["address_generation"],
        observed["address_artifact_sha256"],
        [],
    )
    bundle = SimpleNamespace(
        runtime=SimpleNamespace(
            authorize=RegistryImportedMembershipAdmission(),
            address_profile=queue.RegistryMembershipRefreshAddressProfile("addresses", ("reader",)),
        ),
        inputs_service=AsyncMock(return_value=observed),
    )
    connection = SimpleNamespace(
        fetchrow=AsyncMock(return_value=None), transaction=Mock(side_effect=lambda **_options: nullcontext())
    )
    build, enqueue = AsyncMock(return_value=bundle), AsyncMock()
    monkeypatch.setattr(db, "acquire_driver", lambda: nullcontext(connection))
    monkeypatch.setattr(runtime, "_settings", lambda raw: settings)
    monkeypatch.setattr(runtime, "_build_native_bundle", build)
    monkeypatch.setattr(queue, "enqueue_registry_membership_refresh", enqueue)
    return SimpleNamespace(
        config=config, connection=connection, observed=observed, bundle=bundle, build=build, enqueue=enqueue
    )


@pytest.mark.asyncio
async def test_native_admission_reuses_current_generation(native_import_admission):
    case = native_import_admission
    async with AsyncExitStack() as cleanup:
        assert await importer.admit_latest_network_import(case.config, cleanup, "run-one") == (
            None,
            {
                "state": "unchanged",
                "generation_id": case.observed["expected_serving_generation"],
                "manifest_sha256": case.observed["expected_serving_manifest_sha256"],
            },
        )
    case.connection.transaction.assert_called_once_with(isolation="repeatable_read")
    case.bundle.inputs_service.assert_awaited_once()
    case.enqueue.assert_not_awaited()


@pytest.mark.asyncio
async def test_native_admission_refuses_legacy_authority(native_import_admission):
    from tests.test_registry_membership_refresh_authority import client

    case = native_import_admission
    async with client(lambda _request: pytest.fail("ordinary admission contacted legacy authority")) as authority:
        case.bundle.runtime.authorize = authority
        async with AsyncExitStack() as cleanup:
            with pytest.raises(ValueError, match="native_network_import_required"):
                await importer.admit_latest_network_import(case.config, cleanup, "run-one")
    case.bundle.inputs_service.assert_not_awaited()
    case.enqueue.assert_not_awaited()


@pytest.mark.asyncio
async def test_native_admission_resumes_exact_queued_request(native_import_admission):
    case = native_import_admission
    case.observed.update(address_generation="b" * 64, address_artifact_sha256="c" * 64)
    stored = _queued_refresh_record(case.observed, "run-one") | {"state": "running", "result_json": None}
    case.enqueue.return_value = {"request_id": str(stored["request_id"])}

    async with AsyncExitStack() as cleanup:
        first = await importer.admit_latest_network_import(case.config, cleanup, "run-one")
        case.connection.fetchrow.return_value = stored
        case.bundle.inputs_service.side_effect = AssertionError("a queued replay must use its saved source inputs")
        assert await importer.admit_latest_network_import(case.config, cleanup, "run-one") == first

    assert first == (stored["request_id"], None)
    assert [invocation.args[1:] for invocation in case.connection.fetchrow.await_args_list] == [
        (stored["actor_key"], stored["idempotency_key"]),
        (stored["actor_key"], stored["idempotency_key"]),
    ]
    case.build.assert_awaited_once()
    case.bundle.inputs_service.assert_awaited_once()
    case.enqueue.assert_awaited_once_with(
        case.connection,
        importer.latest_network_refresh_envelope(case.observed, "run-one"),
        authorize=case.bundle.runtime.authorize,
        address_profile=case.bundle.runtime.address_profile,
        control_schema=case.config.consumer.control_schema,
    )


@pytest.fixture
def capture_preparation(monkeypatch):
    from process import registry_imported_source_preparation as preparation
    from process import registry_membership_refresh_runtime as runtime
    from process.registry_imported_source_reuse import ImportedSourcePreparation

    stage = SimpleNamespace(active=False, fail_commit=False, repeatable_read=False)
    connection = object()

    @asynccontextmanager
    async def transaction():
        stage.active = True
        try:
            yield
            if stage.fail_commit:
                raise RuntimeError("synthetic_capture_commit_failed")
        finally:
            stage.active = False
            stage.repeatable_read = False

    async def execute(statement):
        assert stage.active
        assert str(statement) == "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ"
        stage.repeatable_read = True

    async def prepare(capture_transaction, **inputs):
        async with capture_transaction() as driver:
            assert driver is connection and stage.active and stage.repeatable_read
            assert inputs["source_runtime"] is resolver.source_runtime and inputs["control_schema"] == "registry"
        return prepared_sources

    session = SimpleNamespace(begin=transaction, execute=execute)
    sessions = Mock(side_effect=lambda: nullcontext(session))
    resources = SimpleNamespace(settings=SimpleNamespace(control_schema="registry"))
    resolver = runtime._SourceResolver(resources, source_runtime=object(), capture_sessions=sessions)
    prepared_sources = ImportedSourcePreparation((object(), object()), ("new_candidate",), ("retained_candidate",))
    dispatch = AsyncMock(side_effect=prepare)
    monkeypatch.setattr(preparation, "prepare_imported_source_recipes", dispatch)
    monkeypatch.setattr(runtime, "_driver", AsyncMock(return_value=connection))
    return SimpleNamespace(
        resolver=resolver, stage=stage, dispatch=dispatch, sessions=sessions, prepared_sources=prepared_sources
    )


async def _observed_refresh(inventory, connection):
    from process.registry_imported_membership_refresh import imported_catalog_inputs

    _, sources, _ = await imported_catalog_inputs(connection, "registry", inventory=inventory)
    return {**observation(), "accepted_sources": sources}


@pytest.mark.asyncio
@pytest.mark.parametrize("fail_commit", [False, True])
async def test_refresh_prepares_exact_sources_once_after_capture_commit(
    observed_source_inventory, capture_preparation, fail_commit
):
    case, connection = capture_preparation, object()
    observed = await _observed_refresh(observed_source_inventory, connection)
    request = _queued_refresh_record(observed, "first-run")
    if fail_commit:
        case.stage.fail_commit = True
        with pytest.raises(RuntimeError, match="capture_commit_failed"):
            await case.resolver.prepare_sources(connection, request)
        assert case.resolver.prepared_command is None
        assert case.resolver.prepared_recipes == case.resolver.created_schemas == ()
        case.stage.fail_commit = False
        case.dispatch.reset_mock()
    await case.resolver.prepare_sources(connection, request)
    await case.resolver.prepare_sources(connection, request)
    assert case.resolver.prepared_recipes == case.prepared_sources.recipes
    assert case.resolver.created_schemas == ("new_candidate",)
    assert case.dispatch.await_args.kwargs["request_id"] == request["request_id"]
    assert case.dispatch.await_args.kwargs["envelope"]["command"]["accepted_sources"] == observed["accepted_sources"]
    from process.registry_membership_refresh_contract import RegistryMembershipRefreshUnavailable

    with pytest.raises(RegistryMembershipRefreshUnavailable):
        await case.resolver.prepare_sources(connection, _queued_refresh_record(observed, "other-run"))
    case.dispatch.assert_awaited_once()
    assert not case.stage.active


@pytest.mark.asyncio
@pytest.mark.parametrize("incomplete", ["missing-source", "missing-capture"])
async def test_membership_refresh_rejects_incomplete_inputs_before_native_capture(
    observed_source_inventory, capture_preparation, incomplete
):
    from process.registry_approval_store import RegistryApprovalConflict
    from process.registry_membership_refresh_contract import RegistryMembershipRefreshUnavailable

    case, connection = capture_preparation, object()
    observed = await _observed_refresh(observed_source_inventory, connection)
    if incomplete == "missing-source":
        observed["accepted_sources"].pop()
        error = RegistryApprovalConflict
    else:
        case.resolver.capture_sessions = None
        error = RegistryMembershipRefreshUnavailable
    with pytest.raises(error):
        await case.resolver.prepare_sources(connection, _queued_refresh_record(observed, "incomplete-run"))
    assert case.resolver.prepared_command is None
    case.sessions.assert_not_called()
    case.dispatch.assert_not_awaited()


@pytest.fixture
def catalog_loader_lifetime(monkeypatch):
    from sqlalchemy.engine import make_url

    from process import registry_network_import_runtime as runtime

    loader = SimpleNamespace(fetchval=AsyncMock(return_value=True), execute=AsyncMock())
    sealer = object()
    pools_by_role = {"loader": SimpleNamespace(acquire=lambda: nullcontext(loader)), "sealer": sealer}
    lifecycle_events = []

    @asynccontextmanager
    async def pool(url, *, min_size, max_size):
        username = make_url(url).username
        assert min_size == 1 and max_size == (2 if username == "sealer" else 1)
        lifecycle_events.append(("open", username))
        try:
            yield pools_by_role[username]
        finally:
            lifecycle_events.append(("close", username))

    catalog = SimpleNamespace(
        source_url=None,
        loader_url=make_url("postgresql://loader@db.example/catalog"),
        sealer_url=make_url("postgresql://sealer@db.example/catalog"),
        consumer=SimpleNamespace(control_schema="registry"),
    )
    monkeypatch.setattr(runtime.asyncpg, "create_pool", pool)
    return SimpleNamespace(catalog=catalog, loader=loader, sealer=sealer, lifecycle_events=lifecycle_events)


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["reuse", "changed-inventory", "busy"])
async def test_coupled_catalog_reuse_requires_current_inputs_and_releases_loader(
    monkeypatch, observed_source_inventory, catalog_loader_lifetime, mode
):
    from process import registry_fhir_catalog_sync as catalog_sync
    from process import registry_imported_catalog_preparation as preparation
    from process import registry_network_import_runtime as runtime

    case = catalog_loader_lifetime
    case.loader.fetchval.return_value = mode != "busy"
    source_runtime = SimpleNamespace(catalog_export=Mock(), freeze_preparation=AsyncMock())
    prior_snapshot_by_field = {
        "schema_name": "retained_catalog",
        "metadata_generation": "e" * 64,
        "accounting_json": {
            preparation.INPUT_KEY: {"token": observed_source_inventory.token if mode == "reuse" else "f" * 64}
        },
    }

    async def prepare(config, connection, source_pool, sealer_pool, *, source_export):
        assert (config, connection, source_pool, sealer_pool) == (case.catalog, case.loader, None, case.sealer)
        assert source_export is source_runtime.catalog_export
        assert preparation.current_catalog_inventory() is observed_source_inventory
        return None, prior_snapshot_by_field, {"preparation_state": "validated"}

    prepare_catalog = AsyncMock(side_effect=prepare)
    monkeypatch.setattr(catalog_sync, "prepare_fhir_catalog_snapshot", prepare_catalog)
    async with AsyncExitStack() as cleanup:
        failure = {"busy": "catalog_sync_already_running", "changed-inventory": "catalog_preparation_changed"}.get(mode)
        if failure:
            with pytest.raises(ValueError, match=failure):
                await runtime.prepare_network_import_catalog(case.catalog, source_runtime, cleanup)
        else:
            prepared_catalog = await runtime.prepare_network_import_catalog(case.catalog, source_runtime, cleanup)
            assert prepared_catalog.snapshot is prior_snapshot_by_field
            assert prepared_catalog.inventory_token == observed_source_inventory.token
    assert case.lifecycle_events == [("open", "loader"), ("open", "sealer"), ("close", "sealer"), ("close", "loader")]
    assert preparation.current_catalog_inventory() is None
    source_runtime.freeze_preparation.assert_not_awaited()
    if mode == "busy":
        prepare_catalog.assert_not_awaited()
        case.loader.execute.assert_not_awaited()
    else:
        prepare_catalog.assert_awaited_once()
        case.loader.execute.assert_awaited_once_with(
            "SELECT pg_advisory_unlock(hashtextextended($1,0))", "registry-fhir-catalog:registry"
        )
