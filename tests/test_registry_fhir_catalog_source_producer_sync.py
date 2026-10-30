# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure Source export wiring and identity adoption; no native authority is established."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import registry_fhir_catalog_reporting as reporting
from process import registry_fhir_catalog_source_producer as producer
from process import registry_fhir_catalog_sync as sync
from process.registry_fhir_catalog_extraction import fhir_catalog_json_bytes
from process.registry_fhir_catalog_observation import CATALOG_OBSERVATION_CONTRACT
from process.registry_fhir_catalog_source_producer import (
    CATALOG_DATABASE_IDENTITY_SQL,
    iter_source_owned_observation_batches,
)
from tests.test_registry_fhir_catalog_grant import snapshot, standing_grant
from tests.test_registry_fhir_catalog_reporting import disposition
from tests.test_registry_fhir_catalog_source_producer import _boundary
from tests.test_registry_fhir_catalog_source_producer_grant import observed_descriptor
from tests.test_registry_fhir_catalog_sources import descriptor, read_profile, scoped_read_profile

LEGACY_ONE = "11111111-1111-4111-8111-111111111111"
LEGACY_TWO = "22222222-2222-4222-8222-222222222222"


@pytest.fixture
def reconciliation_case(monkeypatch, source_case):
    from sqlalchemy.engine import make_url

    from process.registry_fhir_catalog_publication import FHIRCatalogPublicationRuntime
    from process.registry_publication_consumer import RegistryPublicationConsumerConfig

    config = source_case.config
    config.source_url = None
    for name in ("loader_url", "sealer_url", "lifecycle_url", "execution_url"):
        setattr(config, name, make_url("postgresql://catalog_actor@localhost/catalog"))
    config.consumer = RegistryPublicationConsumerConfig(
        "control_example", "lifecycle_example", "execution_example", "api_example", "queue_example", "owner_example"
    )
    events = []
    source_case.loader.fetchval = AsyncMock(return_value=True)
    pool = SimpleNamespace(acquire=lambda: _scope(source_case.loader))

    @asynccontextmanager
    async def open_pool(*_arguments, **_options):
        events.append("opened")
        try:
            yield pool
        finally:
            events.append("closed")

    monkeypatch.setattr(sync.asyncpg, "create_pool", Mock(side_effect=open_pool))
    monkeypatch.setattr(sync, "verify_fhir_catalog_preparation_permissions", AsyncMock())
    runtime = FHIRCatalogPublicationRuntime(AsyncMock(), AsyncMock(), AsyncMock())
    monkeypatch.setattr(sync, "build_fhir_catalog_publication_runtime", Mock(return_value=runtime))
    monkeypatch.setattr(sync, "prepare_fhir_catalog_snapshot", AsyncMock())
    monkeypatch.setattr(sync, "_publish_catalog_snapshot", AsyncMock())
    return source_case, events


@pytest.mark.asyncio
async def test_reconciliation_contention_closes_pools_without_candidate_or_unlock(reconciliation_case):
    case, events = reconciliation_case
    case.loader.fetchval.return_value = False
    with pytest.raises(ValueError, match="sync_already_running"):
        await sync.reconcile_fhir_catalog(case.config, source_export=case.factory)
    sync.prepare_fhir_catalog_snapshot.assert_not_awaited()
    sync._publish_catalog_snapshot.assert_not_awaited()
    case.loader.execute.assert_not_awaited()
    assert events == ["opened"] * 4 + ["closed"] * 4


@pytest.mark.asyncio
async def test_unchanged_reconciliation_unlocks_without_republication(reconciliation_case):
    case, events = reconciliation_case
    sync.prepare_fhir_catalog_snapshot.return_value = (None, {"retained": True}, {"accepted_sources": 2})
    result = await sync.reconcile_fhir_catalog(case.config, source_export=case.factory)
    assert result == {"state": "unchanged", "accepted_sources": 2}
    sync._publish_catalog_snapshot.assert_not_awaited()
    assert "pg_advisory_unlock" in str(case.loader.execute.await_args.args[0])
    assert events == ["opened"] * 4 + ["closed"] * 4


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [PermissionError, asyncio.CancelledError])
async def test_reconciliation_failure_preserves_error_and_releases_lock(reconciliation_case, failure):
    case, events = reconciliation_case
    error = failure("candidate failed")
    sync.prepare_fhir_catalog_snapshot.side_effect = error
    with pytest.raises(failure) as caught:
        await sync.reconcile_fhir_catalog(case.config, source_export=case.factory)
    assert caught.value is error
    sync._publish_catalog_snapshot.assert_not_awaited()
    assert "pg_advisory_unlock" in str(case.loader.execute.await_args.args[0])
    assert events == ["opened"] * 4 + ["closed"] * 4


@pytest.mark.asyncio
async def test_reconciliation_reports_publication_only_after_preparation(reconciliation_case):
    case, events = reconciliation_case
    sync.prepare_fhir_catalog_snapshot.return_value = ({"sealed": True}, None, {"accepted_sources": 1})
    sync._publish_catalog_snapshot.return_value = {"state": "completed"}
    with reporting.fhir_catalog_run_reporting() as report:
        result = await sync.reconcile_fhir_catalog(case.config, source_export=case.factory)
        assert report.document["publication_state"] == "in_progress"
    assert result == {"state": "completed", "accepted_sources": 1}
    assert events == ["opened"] * 4 + ["closed"] * 4


@pytest.mark.asyncio
async def test_discovery_requires_complete_reader_and_bounds_inventory(source_case):
    with pytest.raises(ValueError, match="source_reader_required"):
        await sync._discover_catalog_sources(None, source_case.config.grant)
    source_case.export["inventory"].side_effect = lambda: _pages((source_case.accepted,) * 1001)
    exports_by_profile = {sync.canonical_json(read_profile().document()): source_case.export}
    with pytest.raises(ValueError, match="source_vector_bounds"):
        await sync._discover_catalog_sources(None, source_case.config.grant, exports_by_profile=exports_by_profile)


@pytest.mark.asyncio
async def test_registry_head_absence_refuses_before_manifest_resolution(monkeypatch):
    loader = SimpleNamespace(fetchrow=AsyncMock(return_value=None))
    monkeypatch.setattr(sync, "resolve_network_serving_manifest", AsyncMock())
    with pytest.raises(ValueError, match="registry_unavailable"):
        await sync._catalog_registry_head(loader, standing_grant())
    sync.resolve_network_serving_manifest.assert_not_awaited()


def test_initial_catalog_requires_a_source_and_exact_export_key(source_case):
    with pytest.raises(ValueError, match="no_accepted_sources"):
        sync._validate_catalog_sources([], None)
    with pytest.raises(ValueError, match="source_export_required"):
        sync._source_catalog_export(source_case.accepted.descriptor, {})


@pytest.mark.asyncio
@pytest.mark.parametrize("selected", [object(), None, "unavailable", "no-reader"])
async def test_staging_refusals_never_write_candidate_rows(monkeypatch, source_case, selected):
    if selected == "unavailable":
        selected = replace(source_case.accepted, status="unavailable")
    elif selected == "no-reader":
        selected = source_case.accepted
    monkeypatch.setattr(sync, "write_fhir_catalog_rows", AsyncMock())
    with pytest.raises(ValueError, match="source_not_accepted|source_reader_required"):
        await sync.stage_fhir_catalog_source(
            None,
            source_case.loader,
            object(),
            selected,
            loader_role="catalog_loader",
            control_schema="catalog_control",
        )
    sync.write_fhir_catalog_rows.assert_not_awaited()


@pytest.mark.asyncio
async def test_current_recipe_replay_checks_current_source_before_reading(monkeypatch):
    selected = descriptor()
    connection = object()
    observe = AsyncMock()
    monkeypatch.setattr(sync, "require_current_fhir_catalog_sources", AsyncMock())
    monkeypatch.setattr(sync, "_published_read", AsyncMock(return_value=None))
    monkeypatch.setattr(sync, "_stage_plan_observations", AsyncMock(return_value=3))
    monkeypatch.setattr(sync, "_stage_reference_observations", AsyncMock(return_value=2))
    assert await sync.replay_fhir_catalog_observations(connection, selected, observe) == 5
    sync.require_current_fhir_catalog_sources.assert_awaited_once_with(connection, (selected,))
    assert sync._stage_plan_observations.await_args.args[2] == selected["coordinates"]["dataset_id"]
    observe.assert_not_awaited()


@pytest.mark.asyncio
async def test_approved_mapping_batch_bound_refuses_instead_of_partial_projection():
    connection = SimpleNamespace(fetch=AsyncMock(return_value=[{"record_kind": "company", "document": "{}"}] * 1001))
    with pytest.raises(ValueError, match="mapping_batch_bounds"):
        await sync._approved_maps(connection, "catalog_control", 6)


@asynccontextmanager
async def _scope(value=None):
    yield value


async def _pages(*pages):
    for page in pages:
        yield page


def _probe_pool(connection):
    events = []

    @asynccontextmanager
    async def acquire():
        events.append("acquire")
        try:
            yield connection
        finally:
            events.append("release")

    @asynccontextmanager
    async def transaction(**options):
        events.append(("begin", options))
        try:
            yield
        except BaseException as error:
            events.append(("rollback", error))
            raise
        else:
            events.append(("commit",))

    connection.transaction = Mock(side_effect=transaction)
    return SimpleNamespace(acquire=Mock(side_effect=acquire)), events


@pytest.fixture
def source_case():
    factory, export, identity_probe, events = _boundary()
    sealer_pool, probe_events = _probe_pool(identity_probe)
    loader = SimpleNamespace(fetchrow=AsyncMock(side_effect=PermissionError("Loader cannot execute pg_control_system")))
    selected = observed_descriptor()
    accepted = replace(disposition(), descriptor=selected, accepted_at=datetime(2026, 1, 1, tzinfo=timezone.utc))
    export["inventory"].side_effect = lambda: _pages((accepted,))
    loader.transaction = Mock(side_effect=lambda **_options: _scope())
    loader.execute = AsyncMock()
    loader.fetch = AsyncMock(return_value=[])
    grant = standing_grant(observation_contract=CATALOG_OBSERVATION_CONTRACT)
    config = SimpleNamespace(grant=grant, loader_role="catalog_loader")
    return SimpleNamespace(
        factory=factory,
        export=export,
        loader=loader,
        events=events,
        accepted=accepted,
        config=config,
        identity_probe=identity_probe,
        sealer_pool=sealer_pool,
        probe_events=probe_events,
    )


def _preparation(monkeypatch, case):
    selected = snapshot(case.config.grant, (case.accepted.descriptor,))
    current_by_generation = {sync.fhir_catalog_source_generation(case.accepted.descriptor): case.accepted}
    monkeypatch.setattr(sync, "_catalog_registry_head", AsyncMock(return_value=({}, None, None)))
    monkeypatch.setattr(sync, "_retained_catalog_sources", AsyncMock(return_value=[]))
    monkeypatch.setattr(sync, "_catalog_snapshot_inputs", AsyncMock(return_value=(selected, current_by_generation)))
    monkeypatch.setattr(sync, "_reusable_catalog_snapshot", AsyncMock(return_value=None))

    async def build(*arguments):
        assert case.events == [("enter", read_profile())]
        assert arguments[2] == (None, None, {sync.canonical_json(read_profile().document()): case.export})
        return {"written": 1}

    async def seal(*_args, **options):
        assert case.events == [("enter", read_profile()), ("exit",)]
        assert case.probe_events[-2:] == [("commit",), "release"]
        assert options == {"source_pool": None, "source_export": case.factory}

    monkeypatch.setattr(sync, "_build_source_catalog_candidate", AsyncMock(side_effect=build))
    monkeypatch.setattr(sync, "_seal_catalog_snapshot", AsyncMock(side_effect=seal))
    monkeypatch.setattr(sync, "discard_fhir_catalog_candidate", AsyncMock())
    monkeypatch.setattr(sync, "iter_fhir_catalog_sources", Mock(side_effect=AssertionError("native inventory")))
    return selected


@pytest.mark.asyncio
async def test_preparation_closes_source_before_sealing_and_records_inventory(monkeypatch, source_case):
    expected = _preparation(monkeypatch, source_case)
    with reporting.fhir_catalog_run_reporting() as report:
        selected, previous, accounting = await sync.prepare_fhir_catalog_snapshot(
            source_case.config, source_case.loader, None, source_case.sealer_pool, source_export=source_case.factory
        )
    assert selected is expected and previous is None
    assert accounting["accepted_sources"] == accounting["written"] == 1
    assert report.document["profiles"][0]["complete"] is True
    assert report.document["inventory_complete"] is True
    source_case.factory.assert_called_once_with(source_profile=read_profile())
    source_case.export["inventory"].assert_called_once_with()
    source_case.loader.fetchrow.assert_not_awaited()
    source_case.identity_probe.fetchrow.assert_awaited_once_with(CATALOG_DATABASE_IDENTITY_SQL)
    source_case.identity_probe.transaction.assert_called_once_with(isolation="repeatable_read", readonly=True)
    assert source_case.probe_events == [
        "acquire",
        ("begin", {"isolation": "repeatable_read", "readonly": True}),
        ("commit",),
        "release",
    ]
    sync._seal_catalog_snapshot.assert_awaited_once()
    sync.discard_fhir_catalog_candidate.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["inventory", "build", "exit"])
@pytest.mark.parametrize("error", [RuntimeError("source failure"), asyncio.CancelledError("source canceled")])
async def test_incomplete_or_failed_source_never_seals(monkeypatch, source_case, phase, error):
    _preparation(monkeypatch, source_case)

    async def incomplete_inventory():
        yield (source_case.accepted,)
        raise error

    @asynccontextmanager
    async def rejected_exit(*, source_profile):
        source_case.events.append(("enter", source_profile))
        try:
            yield source_case.export
            raise error
        finally:
            source_case.events.append(("exit",))

    if phase == "inventory":
        source_case.export["inventory"].side_effect = incomplete_inventory
    elif phase == "build":
        sync._build_source_catalog_candidate.side_effect = error
    else:
        source_case.factory.side_effect = rejected_exit
    with reporting.fhir_catalog_run_reporting() as report:
        with pytest.raises(type(error)) as caught:
            await sync.prepare_fhir_catalog_snapshot(
                source_case.config, source_case.loader, None, source_case.sealer_pool, source_export=source_case.factory
            )
        assert caught.value is error
        assert report.document["profiles"][0]["complete"] is (phase != "inventory")
    sync._seal_catalog_snapshot.assert_not_awaited()
    assert sum(event == ("exit",) for event in source_case.events) == 1
    assert source_case.probe_events[-2:] == [("rollback", error), "release"]
    source_case.loader.fetchrow.assert_not_awaited()
    assert sync.discard_fhir_catalog_candidate.await_count == (0 if phase == "inventory" else 1)


@pytest.mark.asyncio
async def test_source_contexts_are_keyed_by_canonical_profile(source_case):
    grant = source_case.config.grant
    async with sync._catalog_source_reads(None, source_case.sealer_pool, grant, source_case.factory) as reads:
        connection, workspace, exports_by_profile = reads
        assert connection is workspace is None
        assert exports_by_profile == {sync.canonical_json(read_profile().document()): source_case.export}
    source_case.factory.assert_called_once_with(source_profile=read_profile())
    assert source_case.events == [("enter", read_profile()), ("exit",)]


@pytest.mark.asyncio
async def test_multiple_producer_profiles_refuse_before_opening_any_context(source_case):
    other_profile = replace(read_profile(), schema_name="another_payload")
    grant = replace(source_case.config.grant, source_profiles=(read_profile(), other_profile))
    source_pool = SimpleNamespace(acquire=Mock())
    with pytest.raises(ValueError, match="source_export_profiles_unsupported"):
        async with sync._catalog_source_reads(source_pool, source_case.sealer_pool, grant, source_case.factory):
            pytest.fail("multiple contexts would wait for the same bounded registry pool")
    source_case.factory.assert_not_called()
    source_pool.acquire.assert_not_called()
    source_case.sealer_pool.acquire.assert_not_called()


@pytest.mark.asyncio
async def test_scoped_profile_requires_native_reader_without_substitution(source_case):
    grant = replace(source_case.config.grant, source_profiles=(scoped_read_profile(),))
    with pytest.raises(ValueError, match="source_reader_required"):
        async with sync._catalog_source_reads(None, source_case.sealer_pool, grant, source_case.factory):
            pytest.fail("scoped profile admitted without Reader")
    source_case.factory.assert_not_called()
    source_case.loader.fetchrow.assert_not_awaited()
    source_case.sealer_pool.acquire.assert_not_called()


@pytest.mark.asyncio
async def test_native_reader_does_not_open_preparation_sealer_probe(monkeypatch, source_case):
    connection = SimpleNamespace(execute=AsyncMock(), transaction=Mock(side_effect=lambda **_options: _scope()))
    source_pool = SimpleNamespace(acquire=Mock(side_effect=lambda: _scope(connection)))
    workspace = object()
    monkeypatch.setattr(sync, "fhir_catalog_reference_workspace", lambda _connection: _scope(workspace))
    async with sync._catalog_source_reads(
        source_pool, source_case.sealer_pool, source_case.config.grant, None
    ) as reads:
        assert reads == (connection, workspace, None)
    connection.transaction.assert_called_once_with(isolation="repeatable_read", readonly=True)
    connection.execute.assert_awaited_once_with("SET LOCAL search_path = pg_catalog, pg_temp, public")
    source_case.factory.assert_not_called()
    source_case.sealer_pool.acquire.assert_not_called()
    source_case.identity_probe.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["retained", "semantic", "missing-export"])
async def test_required_native_paths_and_missing_exports_fail_before_staging(monkeypatch, source_case, kind):
    selected = descriptor() if kind == "semantic" else observed_descriptor()
    if kind == "retained":
        selected["profile"] = "retained_normalized"
    accepted = replace(source_case.accepted, descriptor=selected)
    monkeypatch.setattr(sync, "stage_fhir_catalog_source", AsyncMock())
    monkeypatch.setattr(sync, "project_fhir_catalog_source", AsyncMock())
    with pytest.raises(ValueError, match="source_(reader|export)_required"):
        await sync._project_catalog_sources(
            source_case.config,
            source_case.loader,
            (None, None, {}),
            object(),
            {},
            None,
            {"generation": accepted},
        )
    sync.stage_fhir_catalog_source.assert_not_awaited()
    sync.project_fhir_catalog_source.assert_not_awaited()


@pytest.mark.asyncio
async def test_matching_source_export_reaches_staging_and_projection(monkeypatch, source_case):
    monkeypatch.setattr(sync, "stage_fhir_catalog_source", AsyncMock(return_value={"source_rows": 7}))
    monkeypatch.setattr(sync, "project_fhir_catalog_source", AsyncMock(return_value={"networks": 2}))
    exports_by_profile = {sync.canonical_json(read_profile().document()): source_case.export}
    receipt = await sync._project_catalog_sources(
        source_case.config,
        source_case.loader,
        (None, None, exports_by_profile),
        object(),
        {},
        None,
        {"generation": source_case.accepted},
    )
    assert receipt == [{"extraction": {"source_rows": 7}, "projection": {"networks": 2}}]
    assert sync.stage_fhir_catalog_source.await_args.kwargs["source_export"] is source_case.export
    assert sync.project_fhir_catalog_source.await_args.kwargs["source_export"] is source_case.export


@pytest.mark.asyncio
@pytest.mark.parametrize("error", [None, RuntimeError("replay incomplete"), asyncio.CancelledError()])
async def test_replay_stages_all_expanded_facts_and_only_complete_source_receipt(monkeypatch, source_case, error):
    generation = sync.fhir_catalog_source_generation(source_case.accepted.descriptor)
    observations = tuple((generation, "InsurancePlan", "plan-one", "edge", ordinal, "{}") for ordinal in range(1001))

    async def replay(selected, collector):
        assert selected is source_case.accepted.descriptor
        for batch in iter_source_owned_observation_batches(observations):
            await collector(batch)
        if error is not None:
            raise error
        return 7

    async def write(_loader, _target, table, rows, **_options):
        size = len(fhir_catalog_json_bytes(rows)) if table == "registry_fhir_catalog_observation" else 0
        return {"input_rows": len(rows), "batches": 1, "payload_bytes": size}

    source_case.export["replay"].side_effect = replay
    monkeypatch.setattr(sync, "write_fhir_catalog_rows", AsyncMock(side_effect=write))
    monkeypatch.setattr(
        sync, "replay_fhir_catalog_observations", AsyncMock(side_effect=AssertionError("native replay"))
    )
    operation = sync.stage_fhir_catalog_source(
        None,
        source_case.loader,
        object(),
        source_case.accepted,
        loader_role="catalog_loader",
        control_schema="catalog_control",
        source_export=source_case.export,
    )
    if error is not None:
        with pytest.raises(type(error)) as caught:
            await operation
        assert caught.value is error
    else:
        receipt = await operation
        assert receipt["source_rows"] == 7 and receipt["observation_rows"] == 1001
    writes = sync.write_fhir_catalog_rows.await_args_list
    facts = [call.args[3] for call in writes if call.args[2] == "registry_fhir_catalog_observation"]
    assert [len(batch) for batch in facts] == [1000, 1]
    assert tuple(observation for batch in facts for observation in batch) == observations
    source_rows = [call.args[3] for call in writes if call.args[2] == "registry_fhir_catalog_source"]
    assert len(source_rows) == (1 if error is None else 0)
    if source_rows:
        assert source_rows[0][0][4:6] == (7, 1001)
    sync.replay_fhir_catalog_observations.assert_not_awaited()


def _binding(resource_id="network-one", network_id=LEGACY_ONE, source_id="alias-a"):
    return {"source_id": source_id, "resource_id": resource_id, "network_id": network_id}


def _alias(identifier=LEGACY_ONE, scope="scope-one", network_id=17):
    return {"alias_value": identifier, "scope_key": scope, "network_id": network_id}


@pytest.mark.asyncio
async def test_batched_source_bindings_preserve_native_uuid_identity_inputs(source_case):
    unmatched = "33333333-3333-4333-8333-333333333333"
    source_bindings = (_binding(), _binding("network-two", LEGACY_TWO, "alias-b"), _binding(network_id=unmatched))
    source_case.export["legacy_bindings"].side_effect = lambda *_args: _pages(
        (source_bindings[0],), source_bindings[1:]
    )
    source_case.loader.fetch.side_effect = [[_alias()], [_alias(LEGACY_TWO, "scope-two", 18)]]
    organization_ids = ("network-one", "network-two")
    exported = await sync._legacy_identity_inputs(
        None,
        source_case.loader,
        source_case.accepted.descriptor,
        organization_ids,
        '"catalog_control"',
        source_export=source_case.export,
    )
    assert exported == {"network-one": (LEGACY_ONE, "scope-one"), "network-two": (LEGACY_TWO, "scope-two")}
    assert source_case.loader.fetch.await_count == 2
    assert [call.args[2] for call in source_case.loader.fetch.await_args_list] == [
        [LEGACY_ONE],
        [LEGACY_TWO, unmatched],
    ]
    assert all(call.args[1] == ["alias-a", "alias-b"] for call in source_case.loader.fetch.await_args_list)
    source_case.export["legacy_bindings"].assert_called_once_with(source_case.accepted.descriptor, organization_ids)
    native = SimpleNamespace(fetch=AsyncMock(return_value=source_bindings))
    source_case.loader.fetch.side_effect = None
    source_case.loader.fetch.return_value = [_alias(), _alias(LEGACY_TWO, "scope-two", 18)]
    original = await sync._legacy_identity_inputs(
        native, source_case.loader, source_case.accepted.descriptor, organization_ids, '"catalog_control"'
    )
    organizations = tuple(SimpleNamespace(resource_id=key) for key in organization_ids)
    assert sync._network_identity_inputs(organizations, source_case.accepted.descriptor, "generation", exported) == (
        sync._network_identity_inputs(organizations, source_case.accepted.descriptor, "generation", original)
    )
    assert original == exported
    native.fetch.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "page",
    [
        (),
        [],
        (_binding(source_id="other-source"),),
        (_binding(resource_id="other-network"),),
        (_binding(network_id="not-a-uuid"),),
        (_binding(network_id="00000000-0000-0000-0000-000000000000"),),
        ({**_binding(), "extra": True},),
        (_binding(),) * 1001,
    ],
    ids=["empty", "not-tuple", "wrong-alias", "nonrequested-org", "invalid-uuid", "nil-uuid", "extra-key", "rows"],
)
async def test_invalid_binding_page_refuses_before_registry_lookup(source_case, page):
    source_case.export["legacy_bindings"].side_effect = lambda *_args: _pages(page)
    with pytest.raises(ValueError):
        await sync._legacy_identity_inputs(
            None,
            source_case.loader,
            source_case.accepted.descriptor,
            ("network-one",),
            '"catalog_control"',
            source_export=source_case.export,
        )
    source_case.loader.fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_binding_page_bytes_include_utf8_and_exact_boundary(monkeypatch, source_case):
    page = (_binding(source_id="alias-é"),)
    source_case.accepted.descriptor["source_aliases"] = ["alias-é"]
    source_case.export["legacy_bindings"].side_effect = lambda *_args: _pages(page)
    size = len(fhir_catalog_json_bytes(page))
    monkeypatch.setattr(producer, "DEFAULT_BYTES", size)
    assert (
        await sync._legacy_identity_inputs(
            None,
            source_case.loader,
            source_case.accepted.descriptor,
            ("network-one",),
            '"catalog_control"',
            source_export=source_case.export,
        )
        == {}
    )
    source_case.loader.fetch.assert_awaited_once()
    source_case.loader.fetch.reset_mock()
    monkeypatch.setattr(producer, "DEFAULT_BYTES", size - 1)
    with pytest.raises(ValueError, match="identity_source_bounds"):
        await sync._legacy_identity_inputs(
            None,
            source_case.loader,
            source_case.accepted.descriptor,
            ("network-one",),
            '"catalog_control"',
            source_export=source_case.export,
        )
    source_case.loader.fetch.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "organization_ids",
    [(), ["network-one"], ("network-one",) * 2, ("a/b",), (True,), tuple(str(i) for i in range(1001))],
)
async def test_invalid_requested_organization_batch_never_starts_source(source_case, organization_ids):
    with pytest.raises(ValueError, match="identity_source_invalid"):
        await sync._legacy_identity_inputs(
            None,
            source_case.loader,
            source_case.accepted.descriptor,
            organization_ids,
            '"catalog_control"',
            source_export=source_case.export,
        )
    source_case.export["legacy_bindings"].assert_not_called()
    source_case.loader.fetch.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("aliases", [[], ["alias-b", "alias-a"], ["alias-a", "alias-a"], [" alias-a"], [1, "alias-a"]])
async def test_invalid_alias_set_never_starts_source(source_case, aliases):
    source_case.accepted.descriptor["source_aliases"] = aliases
    with pytest.raises(ValueError, match="identity_source_invalid"):
        await sync._legacy_identity_inputs(
            None,
            source_case.loader,
            source_case.accepted.descriptor,
            ("network-one",),
            '"catalog_control"',
            source_export=source_case.export,
        )
    source_case.export["legacy_bindings"].assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["uuid-change", "registry-conflict", "incomplete", "cancel"])
async def test_binding_conflicts_or_incomplete_pages_fail_before_allocator(monkeypatch, source_case, failure):
    error = asyncio.CancelledError() if failure == "cancel" else RuntimeError("incomplete bindings")

    async def bindings(*_args):
        yield (_binding(),)
        if failure in {"incomplete", "cancel"}:
            raise error
        yield (_binding(network_id=LEGACY_TWO if failure == "uuid-change" else LEGACY_ONE),)

    source_case.export["legacy_bindings"].side_effect = bindings
    second_page_aliases = (
        [_alias(LEGACY_TWO, "scope-two", 18)]
        if failure == "uuid-change"
        else [_alias(network_id=18), _alias(network_id=19)]
    )
    source_case.loader.fetch.side_effect = [[_alias()], second_page_aliases]
    monkeypatch.setattr(sync, "_network_organization_page", AsyncMock(return_value=[{"resource_id": "network-one"}]))
    monkeypatch.setattr(sync, "allocate_fhir_catalog_network_ids", AsyncMock())
    monkeypatch.setattr(sync, "_published_read", AsyncMock(side_effect=AssertionError("native published read")))
    expected_error = ValueError if failure in {"uuid-change", "registry-conflict"} else type(error)
    with pytest.raises(expected_error) as caught:
        await sync.project_fhir_catalog_source(
            source_case.loader,
            SimpleNamespace(schema_name="catalog_candidate"),
            source_case.accepted,
            {},
            grant=source_case.config.grant,
            loader_role="catalog_loader",
            source_export=source_case.export,
        )
    if failure in {"incomplete", "cancel"}:
        assert caught.value is error
    sync.allocate_fhir_catalog_network_ids.assert_not_awaited()
    sync._published_read.assert_not_awaited()


@pytest.mark.asyncio
async def test_reconcile_requires_reader_or_source_export_before_opening_pools(monkeypatch):
    monkeypatch.setattr(sync.asyncpg, "create_pool", Mock())
    with pytest.raises(ValueError, match="source_reader_required"):
        await sync.reconcile_fhir_catalog(SimpleNamespace(source_url=None))
    sync.asyncpg.create_pool.assert_not_called()


@pytest.mark.asyncio
async def test_producer_only_reconcile_omits_source_pool_and_preserves_publication_keyword(monkeypatch, source_case):
    url = SimpleNamespace(username="catalog_sealer", set=Mock())
    url.set.return_value.render_as_string.return_value = "synthetic://database"
    consumer = SimpleNamespace()
    monkeypatch.setattr(sync, "replace", lambda original, **changes: SimpleNamespace(**vars(original), **changes))
    config = source_case.config
    for name in ("loader_url", "sealer_url", "lifecycle_url", "execution_url"):
        setattr(config, name, url)
    config.source_url, config.consumer = None, consumer
    source_case.loader.fetchval = AsyncMock(return_value=True)
    pool = SimpleNamespace(acquire=lambda: _scope(source_case.loader))
    monkeypatch.setattr(sync.asyncpg, "create_pool", Mock(side_effect=lambda *_args, **_kwargs: _scope(pool)))
    monkeypatch.setattr(sync, "verify_fhir_catalog_preparation_permissions", AsyncMock())
    runtime = object()
    monkeypatch.setattr(sync, "build_fhir_catalog_publication_runtime", Mock(return_value=runtime))
    monkeypatch.setattr(sync, "prepare_fhir_catalog_snapshot", AsyncMock(return_value=({"sealed": True}, None, {})))
    monkeypatch.setattr(sync, "_publish_catalog_snapshot", AsyncMock(return_value={"state": "completed"}))
    assert await sync.reconcile_fhir_catalog(config, source_export=source_case.factory) == {"state": "completed"}
    assert sync.asyncpg.create_pool.call_count == 4
    sync.build_fhir_catalog_publication_runtime.assert_called_once_with(
        config.grant, None, source_export=source_case.factory
    )
    sync.prepare_fhir_catalog_snapshot.assert_awaited_once_with(
        config, source_case.loader, None, pool, source_export=source_case.factory
    )
    assert sync._publish_catalog_snapshot.await_args.args[4] is runtime


@pytest.mark.asyncio
async def test_sealing_verifier_receives_source_factory(monkeypatch, source_case):
    reader = SimpleNamespace(transaction=lambda **_options: _scope())
    sealer = SimpleNamespace(transaction=lambda: _scope(), execute=AsyncMock())
    actors = iter((reader, sealer))
    pool = SimpleNamespace(acquire=lambda: _scope(next(actors)))
    workspace = object()
    monkeypatch.setattr(sync, "fhir_catalog_reference_workspace", lambda connection: _scope(workspace))
    monkeypatch.setattr(sync, "build_fhir_catalog_observation_verifier", Mock(return_value=object()))
    monkeypatch.setattr(sync, "freeze_fhir_catalog_snapshot", AsyncMock(return_value={"validation": {"tables": {}}}))
    config = source_case.config
    config.consumer = SimpleNamespace(physical_owner_role="catalog_owner")
    config.reader_roles = ("catalog_reader",)
    await sync._seal_catalog_snapshot(config, object(), snapshot(config.grant), pool, source_export=source_case.factory)
    sync.build_fhir_catalog_observation_verifier.assert_called_once_with(
        config.grant, None, registry_reader=reader, registry_workspace=workspace, source_export=source_case.factory
    )
    sealer.execute.assert_awaited_once()
