# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native approved snapshots and fences; adapter doubles exercise only common contracts."""

import asyncio
import copy
import sys
from dataclasses import replace
from types import SimpleNamespace

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process.network_approved_membership_source import pin_approved_membership_source
from process.registry_company_approval_fence import require_registry_company_approval_fence
from process.registry_refresh_anchor_contract import (
    RegistryRefreshAnchorMismatch,
    RegistryRefreshAnchorStale,
    RegistryRefreshAnchorUnavailable,
)
from process.registry_refresh_anchor_service import RegistryRefreshAnchorService, read_approved_refresh_bindings
from tests.test_network_approved_source_bindings_postgres import _setup
from tests.test_network_approved_source_bindings_postgres import serving_schema as serving_schema

pytestmark = pytest.mark.asyncio


def scope_for_binding(binding):
    native = binding.record_json
    return {
        "binding_id": binding.binding_id,
        "binding_revision": binding.binding_revision,
        "network_id": binding.network_id,
        "continuity_policy": "exact_capture",
        "source_scope": {
            **{
                name: native[name]
                for name in ("source_system", "source_id", "dataset_schema", "producer_id", "source_key")
            },
            "approved_binding_sha256": binding.approved_binding_sha256,
            "capture_generation": "c" * 64,
        },
    }


def initial_scope(binding, network):
    return {
        "binding_id": binding["binding_id"],
        "binding_revision": 1,
        "network_id": network["record_id"],
        "continuity_policy": "exact_capture",
        "source_scope": {
            **{
                name: binding[name]
                for name in ("source_system", "source_id", "dataset_schema", "producer_id", "source_key")
            },
            "approved_binding_sha256": "a" * 64,
            "capture_generation": "c" * 64,
        },
    }


@pytest.fixture
async def anchor_db(serving_schema, monkeypatch, request):
    connection, schema, base_engine = serving_schema
    _, network, drafts, approved = await _setup(serving_schema, count=getattr(request, "param", 1))
    engine = create_async_engine(base_engine.url, pool_size=1, max_overflow=0, pool_timeout=1)
    sessions = async_sessionmaker(engine, expire_on_commit=False)
    requested_scopes = sorted(
        [initial_scope(draft, network) for draft in drafts], key=lambda scope: scope["binding_id"]
    )
    async with connection.transaction(isolation="repeatable_read"):
        native = await read_approved_refresh_bindings(connection, approved, requested_scopes, control_schema=schema)
    scopes = [scope_for_binding(binding) for binding in native]
    native_calls = []

    async def derive(session, bindings, *, store, read_budget, approved_source):
        await require_registry_company_approval_fence(session, schema)
        assert await session.scalar(text("SHOW transaction_read_only")) == "off"
        assert approved_source.approved_revision == approved.approved_revision and store is not None
        native_calls.append((bindings, read_budget))
        return {binding.binding_id: scope_for_binding(binding)["source_scope"] for binding in bindings}

    monkeypatch.setitem(
        sys.modules, "process.registry_ptg_refresh_anchors", SimpleNamespace(resolve_ptg_refresh_anchors=derive)
    )
    service = RegistryRefreshAnchorService(sessions, schema, ptg_store=object())
    try:
        yield SimpleNamespace(
            connection=connection,
            schema=schema,
            sessions=sessions,
            service=service,
            approved=approved,
            scopes=scopes,
            native=native,
            calls=native_calls,
            engine=engine,
            derive=derive,
        )
    finally:
        await engine.dispose()


def command(fixture, **changes):
    return {
        "approved_revision": fixture.approved.approved_revision,
        "enrolled_scopes": copy.deepcopy(fixture.scopes),
        **changes,
    }


async def test_native_binding_fingerprint_matches_existing_recipe_and_full_pin(anchor_db):
    fixture = anchor_db
    receipt = await fixture.service.verify(command(fixture))
    assert receipt == {
        "approved_revision": fixture.approved.approved_revision,
        "approved_membership_sha256": fixture.approved.generation_id,
        "enrolled_scopes": fixture.scopes,
    }
    actual = await fixture.connection.fetchval(
        f"SELECT encode(sha256(convert_to(jsonb_build_array(record_kind,record_key,record_revision,custom_revision,record_json)::text,'UTF8')),'hex') FROM \"{fixture.schema}\".registry_approved_record WHERE approved_revision=$1 AND record_kind='network_binding'",
        fixture.approved.approved_revision,
    )
    assert actual == fixture.native[0].approved_binding_sha256
    assert fixture.calls[0][0][0] == fixture.native[0]
    assert fixture.calls[0][0][0].custom_revision > 0
    await fixture.service.verify(command(fixture))
    assert fixture.calls[0][1] is not fixture.calls[1][1]
    assert fixture.engine.pool.checkedout() == 0


@pytest.mark.parametrize(
    "field,changed",
    [("binding_revision", 2), ("network_id", 2147483647), ("binding_id", "30000000-0000-0000-0000-000000000001")],
)
async def test_exact_missing_binding_revision_or_network_refuses_whole_result(anchor_db, field, changed):
    fixture = anchor_db
    document = command(fixture)
    document["enrolled_scopes"][0][field] = changed
    with pytest.raises(RegistryRefreshAnchorMismatch):
        await fixture.service.verify(document)
    assert fixture.calls == []


async def test_current_revision_staleness_and_native_nonbinding_drift(anchor_db):
    fixture = anchor_db
    with pytest.raises(RegistryRefreshAnchorStale):
        await fixture.service.verify(command(fixture, approved_revision=fixture.approved.approved_revision + 1))
    await fixture.connection.execute(
        f"UPDATE \"{fixture.schema}\".registry_approved_record SET record_json=record_json||'{{\"unrelated_native_field\":true}}'::jsonb WHERE record_kind='network'"
    )
    receipt = await fixture.service.verify(command(fixture))
    assert receipt["approved_membership_sha256"] != fixture.approved.generation_id


@pytest.mark.parametrize("fault", ["partial", "extra", "wrong_source", "wrong_hash", "mismatch", "unavailable"])
async def test_adapter_must_prove_complete_native_batch_without_queued_json(anchor_db, monkeypatch, fault):
    fixture = anchor_db

    async def malformed(_session, bindings, **_capability):
        if fault == "unavailable":
            raise RegistryRefreshAnchorUnavailable("native bridge absent")
        scopes_by_id = {binding.binding_id: scope_for_binding(binding)["source_scope"] for binding in bindings}
        if fault == "partial":
            return {}
        if fault == "extra":
            return {**scopes_by_id, "unknown": {}}
        selected = scopes_by_id[bindings[0].binding_id]
        if fault == "wrong_source":
            selected["producer_id"] = "other-producer"
        if fault == "wrong_hash":
            selected["approved_binding_sha256"] = "f" * 64
        if fault == "mismatch":
            selected["capture_generation"] = "d" * 64
        return scopes_by_id

    monkeypatch.setitem(
        sys.modules, "process.registry_ptg_refresh_anchors", SimpleNamespace(resolve_ptg_refresh_anchors=malformed)
    )
    expected = RegistryRefreshAnchorMismatch if fault == "mismatch" else RegistryRefreshAnchorUnavailable
    with pytest.raises(expected):
        await fixture.service.verify(command(fixture))
    assert fixture.engine.pool.checkedout() == 0


async def test_absent_host_capability_and_invalid_factory_fail_closed(anchor_db):
    fixture = anchor_db
    with pytest.raises(RegistryRefreshAnchorUnavailable):
        await replace(fixture.service, ptg_store=None).verify(command(fixture))
    with pytest.raises(ValueError):
        RegistryRefreshAnchorService(lambda: fixture.sessions(), fixture.schema)
    with pytest.raises(RegistryRefreshAnchorUnavailable):
        replace(fixture.service, source_reader_sessions=fixture.sessions)


async def test_source_children_are_real_readonly_and_repin_same_native_map(anchor_db, monkeypatch):
    fixture = anchor_db
    native = fixture.native[0]
    await fixture.connection.execute(
        f"UPDATE \"{fixture.schema}\".registry_approved_record SET record_json=jsonb_set(record_json,'{{source_system}}','\"aca\"') WHERE record_kind='network_binding'"
    )
    child_engine = create_async_engine(fixture.engine.url, pool_size=1, max_overflow=0, pool_timeout=1)
    child_sessions = async_sessionmaker(child_engine, expire_on_commit=False)
    scope_by_field = {**fixture.scopes[0]["source_scope"], "source_system": "aca"}
    requested_scopes = [
        {**fixture.scopes[0], "source_scope": scope_by_field, "continuity_policy": "retained_successor"}
    ]
    async with fixture.connection.transaction(isolation="repeatable_read"):
        approved = await pin_approved_membership_source(
            fixture.connection, approved_revision=fixture.approved.approved_revision, control_schema=fixture.schema
        )
        refreshed = await read_approved_refresh_bindings(
            fixture.connection, approved, requested_scopes, control_schema=fixture.schema
        )
    requested_scopes[0]["source_scope"]["approved_binding_sha256"] = refreshed[0].approved_binding_sha256
    calls = []

    async def derive(session, bindings, *, store, read_budget):
        assert store is not None and read_budget is not None
        assert await session.scalar(text("SHOW transaction_read_only")) == "on"
        assert await session.scalar(text("SHOW transaction_isolation")) == "repeatable read"
        assert bindings[0].record_key == native.record_key
        calls.append(bindings)
        return {bindings[0].binding_id: requested_scopes[0]["source_scope"]}

    monkeypatch.setitem(
        sys.modules, "process.registry_aca_refresh_anchors", SimpleNamespace(resolve_aca_refresh_anchors=derive)
    )
    service = replace(fixture.service, source_reader_sessions=child_sessions, aca_store=object())
    try:
        receipt = await service.verify(
            {"approved_revision": approved.approved_revision, "enrolled_scopes": requested_scopes}
        )
        assert receipt["approved_membership_sha256"] == approved.generation_id and len(calls) == 1
        assert fixture.engine.pool.checkedout() == child_engine.pool.checkedout() == 0
    finally:
        await child_engine.dispose()


async def test_cancellation_releases_native_session_and_company_fence(anchor_db, monkeypatch):
    fixture = anchor_db
    entered = asyncio.Event()

    async def suspended(*_arguments, **_capability):
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setitem(
        sys.modules, "process.registry_ptg_refresh_anchors", SimpleNamespace(resolve_ptg_refresh_anchors=suspended)
    )
    operation = asyncio.create_task(fixture.service.verify(command(fixture)))
    await asyncio.wait_for(entered.wait(), timeout=2)
    operation.cancel()
    with pytest.raises(asyncio.CancelledError):
        await operation
    assert fixture.engine.pool.checkedout() == 0
    monkeypatch.setitem(
        sys.modules, "process.registry_ptg_refresh_anchors", SimpleNamespace(resolve_ptg_refresh_anchors=fixture.derive)
    )
    assert (await fixture.service.verify(command(fixture)))["enrolled_scopes"] == fixture.scopes


@pytest.mark.parametrize("anchor_db", [2], indirect=True)
async def test_partial_native_batch_cannot_authorize_any_selected_binding(anchor_db, monkeypatch):
    fixture = anchor_db

    async def one_of_two(_session, bindings, **_capability):
        return {bindings[0].binding_id: scope_for_binding(bindings[0])["source_scope"]}

    monkeypatch.setitem(
        sys.modules, "process.registry_ptg_refresh_anchors", SimpleNamespace(resolve_ptg_refresh_anchors=one_of_two)
    )
    with pytest.raises(RegistryRefreshAnchorUnavailable):
        await fixture.service.verify(command(fixture))


@pytest.mark.parametrize("anchor_db", [2], indirect=True)
async def test_native_bound_covers_complete_batch_before_json_export(anchor_db):
    fixture = anchor_db
    await fixture.connection.execute(
        f"UPDATE \"{fixture.schema}\".registry_approved_record SET record_json=record_json||jsonb_build_object('padding',repeat('x',4300000)) WHERE record_kind='network_binding'"
    )
    with pytest.raises(RegistryRefreshAnchorUnavailable):
        await fixture.service.verify(command(fixture))
    assert fixture.calls == []


async def test_other_database_target_and_non_singleton_factory_are_refused(anchor_db):
    fixture = anchor_db
    other = create_async_engine(
        fixture.engine.url.set(database="other_synthetic_database"), pool_size=1, max_overflow=0
    )
    non_singleton = create_async_engine(fixture.engine.url, pool_size=2, max_overflow=0)
    try:
        with pytest.raises(RegistryRefreshAnchorUnavailable):
            replace(fixture.service, source_reader_sessions=async_sessionmaker(other))
        with pytest.raises(RegistryRefreshAnchorUnavailable):
            replace(fixture.service, source_reader_sessions=async_sessionmaker(non_singleton))
    finally:
        await other.dispose()
        await non_singleton.dispose()
