# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Preparation selectors, native-read seams and atomic service ordering without sockets."""

import copy
import hashlib
import json
import struct
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import registry_ptg_office_capture as capture
from process import registry_ptg_office_preparation as preparation
from process import registry_ptg_office_preparation_contract as contract
from process import registry_ptg_office_preparation_producer as producer
from process.registry_ptg_graph_reader import RegistryPTGGraphReadBudget
from process.registry_ptg_scope_engine import RegistryPTGScopeDeadlineExpired

ID = "11111111-1111-4111-8111-111111111111"
BINDING = "22222222-2222-4222-8222-222222222222"
LOCATION = "33333333-3333-4333-8333-333333333333"


def selector():
    return {
        "capture_id": ID,
        "scope_id": ID,
        "client_id": "synthetic_client",
        "scope_approval_sha256": "a" * 64,
        "retained_generation_id": 3,
        "reason": "Reviewed this exact office",
        "idempotency_key": "office-1",
        "selections": [
            {"binding_id": BINDING, "binding_revision": 4, "dense_source_key": 0, "source_record_ordinal": 7}
        ],
    }


def envelope():
    return {
        "actor": {"kind": "platform_admin", "user_id": ID, "client_id": "system"},
        "session_token_sha256": "b" * 64,
        "selector": selector(),
        "read_limits": {"maximum_bytes": 1024, "maximum_pages": 10, "maximum_coordinates": 10},
    }


def selected_record():
    return {
        **selector()["selections"][0],
        "binding_id": UUID(BINDING),
        "ordinal": 1,
        "record_json": {
            "binding_id": BINDING,
            "revision": 4,
            "archived": False,
            "source_generation": 3,
            "provider_system": "npi",
            "provider_id": "1234567893",
            "location_id": LOCATION,
            "location_key": "c" * 64,
            "address_row_sha256": "d" * 64,
        },
        "provider_group_ref": "e" * 32,
        "provider_group_key": 8,
        "npi_key": 9,
    }


def scope():
    return {
        "scope_id": ID,
        "client_id": "synthetic_client",
        "approval_sha256": "a" * 64,
        "approved_revision": 4,
        "binding_source_key": "source_example",
        "snapshot_id": "snapshot_example",
        "coordinates": {"source_system": "ptg", "source_id": "source_example"},
        "source_scope": {"scope_id": ID, "snapshot_id": "snapshot_example"},
        "evidence": {"graph_identity": {"snapshot_key": 17}, "selected_dense_source_keys": [0]},
    }


@pytest.mark.parametrize(
    "mutation",
    [
        lambda value: value.update(actor={}),
        lambda value: value.update(recipes=[]),
        lambda value: value.update(client_id="system"),
        lambda value: value.update(retained_generation_id=True),
        lambda value: value.update(selections=[]),
        lambda value: value["selections"][0].update(dense_source_key=True),
        lambda value: value["selections"][0].update(source_record_ordinal=-1),
        lambda value: value["selections"].append(copy.deepcopy(value["selections"][0])),
    ],
)
def test_selectors_reject_authority_and_invalid_or_duplicate_choices(mutation):
    document = selector()
    mutation(document)
    with pytest.raises((ValueError, TypeError)):
        contract.validated_prepare_selector(document)


def test_selectors_freeze_original_and_recovery_handles_only():
    document = selector()
    frozen = contract.validated_prepare_selector(document)
    document["selections"][0]["source_record_ordinal"] = 99
    assert frozen["selections"][0]["source_record_ordinal"] == 7
    handles_by_field = {name: frozen[name] for name in contract._HANDLES}
    assert contract.validated_prepare_selector(handles_by_field, read=True) == handles_by_field
    with pytest.raises(ValueError):
        contract.validated_prepare_selector(frozen, read=True)


@pytest.mark.parametrize(
    "actor",
    [
        {"kind": "client", "user_id": ID, "client_id": "synthetic_client"},
        {"kind": "platform_admin", "user_id": ID, "client_id": "synthetic_client"},
    ],
)
def test_preparation_refuses_nonoriginal_admin_actor(actor):
    document = envelope()
    document["actor"] = actor
    with pytest.raises(PermissionError):
        contract.validated_preparation_envelope(document)


@pytest.mark.asyncio
async def test_producer_uses_exact_selection_one_graph_batch_and_no_native_encode(monkeypatch):
    record = selected_record()
    context = SimpleNamespace(graph_identity={"snapshot_key": 17}, source_specification=object())
    selected = AsyncMock(return_value=[record])
    graph = AsyncMock(return_value=b"verified")
    monkeypatch.setattr(producer, "_selected_records", selected)
    monkeypatch.setattr(producer, "verify_registry_ptg_graph_page", graph)
    monkeypatch.setattr(capture, "_encoder", lambda: pytest.fail("producer must not encode a second time"))
    budget = RegistryPTGGraphReadBudget(1024)
    raw, digest, count = await producer.produce_reviewed_office_rows(object(), context, scope(), selector(), budget)
    rows = json.loads(raw)
    assert count == 1 and rows[0]["location_id"] == LOCATION
    assert rows[0]["office_evidence_kind"] == "reviewed_exact_office"
    assert rows[0]["company_key"] is None and rows[0]["cohort_id"] is None
    assert set(rows[0]) == set(capture.COPY_COLUMNS)
    assert digest == hashlib.sha256(capture._canonical(rows[0]) + b"\n").hexdigest()
    page = graph.await_args.args[2]
    assert page["selected_edges"] == struct.pack(">II", 8, 9)
    assert graph.await_args.kwargs["read_budget"] is budget and graph.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("archived", True),
        ("revision", 5),
        ("source_generation", 4),
        ("provider_system", "provider_directory"),
        ("binding_id", ID),
    ],
)
async def test_changed_approved_binding_refuses_before_graph_or_copy(monkeypatch, field, value):
    record = selected_record()
    record["record_json"][field] = value
    monkeypatch.setattr(producer, "_selected_records", AsyncMock(return_value=[record]))
    graph = AsyncMock()
    monkeypatch.setattr(producer, "verify_registry_ptg_graph_page", graph)
    with pytest.raises(ValueError):
        await producer.produce_reviewed_office_rows(
            object(), SimpleNamespace(), scope(), selector(), RegistryPTGGraphReadBudget(1024)
        )
    assert graph.await_count == 0


@pytest.mark.asyncio
async def test_selection_query_refuses_missing_or_ambiguous(monkeypatch):
    monkeypatch.setattr(producer.source, "_physical_binding", AsyncMock(return_value=None))
    rows = []
    execute = AsyncMock(return_value=SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: rows)))
    context = SimpleNamespace(
        source_specification=SimpleNamespace(ptg_schema_name="synthetic_ptg"),
        control_schema="synthetic_control",
        graph_identity={"snapshot_key": 17},
    )
    for unmatched in ([], [selected_record(), selected_record()]):
        rows[:] = unmatched
        with pytest.raises(ValueError, match="selection_unresolved"):
            await producer._selected_records(SimpleNamespace(execute=execute), context, scope(), selector())
    query, parameters = execute.await_args.args
    assert "registry_approved_record" in str(query) and "registry_site_binding" not in str(query)
    assert parameters["approved_revision"] == 4 and parameters["row_limit"] == 2
    assert "ptg2_provider_group_tax_identity_source" in str(query)
    assert parameters["selected_sources"] == (0,)


def test_replay_requires_full_regenerated_row_digest_reason_and_retained_generation(monkeypatch):
    command_by_field = {**selector(), "canonical_input_sha256": "e" * 64, "input_row_count": 1}
    monkeypatch.setattr(preparation, "_reply", lambda descriptor: {"command": command_by_field})
    descriptor = object()
    assert preparation._checked_selection(descriptor, selector(), "e" * 64, 1) is descriptor
    for changes in (
        {"reason": "Changed"},
        {"retained_generation_id": 9},
        {"canonical_input_sha256": "f" * 64},
        {"input_row_count": 2},
    ):
        previous = command_by_field.copy()
        command_by_field.update(changes)
        with pytest.raises(ValueError, match="idempotency_conflict"):
            preparation._checked_selection(descriptor, selector(), "e" * 64, 1)
        command_by_field.clear()
        command_by_field.update(previous)


def _service(monkeypatch, *, commit_failure=False):
    calls = []
    scope_service = SimpleNamespace(store=SimpleNamespace(control_schema="synthetic_control"), reader_sessions=object())
    service = object.__new__(preparation.RegistryPTGOfficePreparationService)
    object.__setattr__(service, "scope_service", scope_service)
    object.__setattr__(service, "publisher_sessions", object())
    session = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(scalar_one=lambda: "original")))

    @asynccontextmanager
    async def transaction(sessions, **kwargs):
        calls.append("reader" if sessions is scope_service.reader_sessions else "publisher")
        yield session
        calls.append("commit")
        if commit_failure:
            raise RuntimeError("commit acknowledgement lost")

    async def context(*args, **kwargs):
        calls.append("context")
        return object(), {"operation": "source"}

    async def authorize(actual_service, actual_envelope, command, deadline):
        calls.append("authorize-" + command["operation"])

    async def prepare(*args):
        calls.append("prepare")
        return object(), "e" * 64, 1

    monkeypatch.setattr(preparation, "registry_company_approval_transaction", transaction)
    monkeypatch.setattr(preparation, "_source_context", context)
    monkeypatch.setattr(preparation, "_authorize", authorize)
    monkeypatch.setattr(preparation, "_prepare_capture", prepare)
    monkeypatch.setattr(preparation, "_reserve_capacity", AsyncMock())
    monkeypatch.setattr(preparation, "_reply", lambda descriptor: {"command": {"operation": "office"}})
    return service, calls


@pytest.mark.asyncio
async def test_service_authorizes_source_before_prepare_and_whole_office_before_commit(monkeypatch):
    service, calls = _service(monkeypatch)
    await service.prepare(envelope())
    assert calls == ["publisher", "context", "authorize-source", "prepare", "authorize-office", "commit"]


@pytest.mark.asyncio
async def test_lost_commit_ack_is_unknown_without_second_prepare(monkeypatch):
    service, calls = _service(monkeypatch, commit_failure=True)
    with pytest.raises(RegistryPTGScopeDeadlineExpired) as observed:
        await service.prepare(envelope())
    assert observed.value.outcome_unknown is True and calls.count("prepare") == 1


@pytest.mark.asyncio
async def test_denied_preparation_never_reaches_copy_or_commit(monkeypatch):
    service, calls = _service(monkeypatch)
    monkeypatch.setattr(preparation, "_authorize", AsyncMock(side_effect=PermissionError("revoked")))
    with pytest.raises(PermissionError):
        await service.prepare(envelope())
    assert calls == ["publisher", "context"]


@pytest.mark.asyncio
async def test_existing_capture_recovery_uses_reader_and_never_recreates_it(monkeypatch):
    service, calls = _service(monkeypatch)

    async def exists(*args):
        calls.append("exists")
        return None, "e" * 64, 1

    read = AsyncMock(return_value=object())
    monkeypatch.setattr(preparation, "_prepare_capture", exists)
    monkeypatch.setattr(preparation, "_read_capture", read)
    monkeypatch.setattr(preparation, "_checked_selection", lambda descriptor, *args: descriptor)
    await service.prepare(envelope())
    assert calls == [
        "publisher",
        "context",
        "authorize-source",
        "exists",
        "commit",
        "reader",
        "context",
        "authorize-source",
        "authorize-office",
        "commit",
    ]
    assert read.await_count == 1


@pytest.mark.asyncio
async def test_read_recovers_under_reader_and_fresh_authority(monkeypatch):
    service, calls = _service(monkeypatch)
    document = envelope()
    document["selector"] = {name: document["selector"][name] for name in contract._HANDLES}
    read_capture = AsyncMock(return_value=object())
    monkeypatch.setattr(preparation, "_read_capture", read_capture)
    await service.read(document)
    assert calls == ["reader", "context", "authorize-source", "authorize-office", "commit"]
    assert read_capture.await_count == 1


@pytest.mark.asyncio
async def test_generated_office_denial_rolls_back_preparation(monkeypatch):
    service, calls = _service(monkeypatch)

    async def authorize(actual_service, actual_envelope, command, deadline):
        calls.append("authorize-" + command["operation"])
        if command["operation"] == "office":
            raise PermissionError("revoked")

    monkeypatch.setattr(preparation, "_authorize", authorize)
    with pytest.raises(PermissionError):
        await service.prepare(envelope())
    assert calls == ["publisher", "context", "authorize-source", "prepare", "authorize-office"]


@pytest.mark.asyncio
async def test_busy_capture_never_reads_or_copies(monkeypatch):
    driver = SimpleNamespace(fetchval=AsyncMock(return_value=False))
    monkeypatch.setattr(capture, "_office_driver", AsyncMock(return_value=driver))
    scope_read = AsyncMock()
    copy = AsyncMock()
    monkeypatch.setattr(capture, "_scope", scope_read)
    monkeypatch.setattr(capture, "prepare_registry_ptg_office_capture", copy)
    with pytest.raises(ValueError, match="preparation_busy"):
        await preparation._prepare_capture(object(), object(), selector(), RegistryPTGGraphReadBudget(1024))
    assert scope_read.await_count == copy.await_count == 0


@pytest.mark.asyncio
async def test_authorization_binds_original_actor_and_command():
    authority = SimpleNamespace(authorize=AsyncMock())
    service = SimpleNamespace(scope_service=SimpleNamespace(authority=authority))
    document = envelope()
    command_by_field = {"operation": "synthetic_source_review"}
    deadline = preparation._deadline(None)
    await preparation._authorize(service, document, command_by_field, deadline)
    assert authority.authorize.await_args.args == (
        {
            "actor": document["actor"],
            "session_token_sha256": document["session_token_sha256"],
            "command": command_by_field,
        },
    )
    assert authority.authorize.await_args.kwargs == {"deadline": deadline}


@pytest.mark.asyncio
async def test_recovery_checks_custody_before_manifest(monkeypatch):
    calls = []

    async def fetch(*args):
        calls.append("manifest")
        return None

    async def custody(*args):
        calls.append("custody")

    driver = SimpleNamespace(execute=AsyncMock(), fetchval=fetch)
    monkeypatch.setattr(capture, "_office_driver", AsyncMock(return_value=driver))
    monkeypatch.setattr(capture, "_custody", custody)
    with pytest.raises(ValueError, match="manifest_changed"):
        await preparation._read_capture(object(), object(), selector(), RegistryPTGGraphReadBudget(1024))
    assert calls == ["custody", "manifest"]


def test_produced_rows_match_compiled_native_codec():
    native = pytest.importorskip("ptg2_address_canon")
    from process import registry_ptg_published_office_scope as published
    from tests.test_registry_ptg_published_office_capture import _fixture

    _, context, document = _fixture()
    scope_by_field = published._office_scope(document, context.graph_identity, [0])
    selected = selected_record()
    fields = producer._site(selected, selector()["selections"][0], 3, 1)
    office = producer._office_row(selected, fields, scope_by_field, selector(), 1)
    raw = capture._canonical([office])
    digest = hashlib.sha256(capture._canonical(office) + b"\n").hexdigest()
    request = capture.RegistryPTGOfficeCaptureRequest(
        UUID(ID),
        digest,
        1,
        "reviewed_exact_office",
        3,
        selector()["reason"],
        "office-1",
    )
    copy_bytes, canonical, count, last, decoded_rows = capture._batch(
        native.encode_registry_ptg_capture_batch,
        raw,
        capture._codec_context(scope_by_field, request),
        0,
    )
    assert copy_bytes.startswith(b"PGCOPY\n\xff\r\n\x00")
    assert count == last == 1 and decoded_rows == [office]
    assert hashlib.sha256(canonical).hexdigest() == digest


@pytest.mark.asyncio
async def test_installed_sessions_require_distinct_roles_same_database():
    from dataclasses import replace

    from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

    from process.registry_ptg_office_custody import RegistryPTGOfficeCustodyProfile
    from tests.test_registry_ptg_scope_engine import _service as scope_fixture

    engines = [
        create_async_engine(url, pool_size=1, max_overflow=0)
        for url in (
            "postgresql+asyncpg://office_reader@127.0.0.1/synthetic",
            "postgresql+asyncpg://office_publisher@127.0.0.1/synthetic",
            "postgresql+asyncpg://office_publisher@127.0.0.1/other_synthetic",
        )
    ]
    try:
        base, _ = scope_fixture()
        scope = replace(
            base,
            reader_sessions=async_sessionmaker(engines[0]),
            office_custody=RegistryPTGOfficeCustodyProfile("office_owner", "office_publisher"),
        )
        service = preparation.RegistryPTGOfficePreparationService(scope, async_sessionmaker(engines[1]))
        assert service.scope_service is scope
        with pytest.raises(ValueError, match="service_unconfigured"):
            preparation.RegistryPTGOfficePreparationService(scope, async_sessionmaker(engines[2]))
        with pytest.raises(ValueError, match="service_unconfigured"):
            preparation.RegistryPTGOfficePreparationService(scope, scope.reader_sessions)
    finally:
        for engine in engines:
            await engine.dispose()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "families,existing,accepted", [(127, False, True), (128, False, False), (128, True, True), (129, True, False)]
)
async def test_capacity_uses_fresh_reader_with_owner_lock(monkeypatch, families, existing, accepted):
    calls = []
    publisher = object()
    reader = SimpleNamespace(execute=AsyncMock())
    publisher_driver = SimpleNamespace(fetchval=AsyncMock(return_value=True))
    reader_driver = SimpleNamespace(
        execute=AsyncMock(), fetchrow=AsyncMock(return_value={"families": families, "existing": existing})
    )

    async def driver(session):
        if session is reader:
            assert reader.execute.await_count == 1
        calls.append("publisher-lock" if session is publisher else "reader-count")
        return publisher_driver if session is publisher else reader_driver

    @asynccontextmanager
    async def transaction(*args, **kwargs):
        calls.append("fresh-reader")
        yield reader

    monkeypatch.setattr(capture, "_driver", driver)
    monkeypatch.setattr(preparation, "registry_company_approval_transaction", transaction)
    service = SimpleNamespace(scope_service=SimpleNamespace(reader_sessions=object()))
    context = SimpleNamespace(control_schema="synthetic_control", owner_role="office_owner")
    if accepted:
        await preparation._reserve_capacity(service, publisher, context, selector())
    else:
        with pytest.raises(ValueError, match="capacity_exhausted"):
            await preparation._reserve_capacity(service, publisher, context, selector())
    assert calls == ["publisher-lock", "fresh-reader", "reader-count"]
    assert (
        str(reader.execute.await_args.args[0])
        == "SELECT pg_catalog.set_config('search_path','pg_catalog,pg_temp',true)"
    )
    assert not reader_driver.execute.await_count
    assert "SELECT pg_catalog.count(*)" in reader_driver.fetchrow.await_args.args[0]


@pytest.mark.asyncio
async def test_capacity_lock_covers_owner_across_control_schemas(monkeypatch):
    publisher, reader = object(), SimpleNamespace(execute=AsyncMock())
    publisher_driver = SimpleNamespace(fetchval=AsyncMock(return_value=True))
    reader_driver = SimpleNamespace(
        execute=AsyncMock(), fetchrow=AsyncMock(return_value={"families": 127, "existing": False})
    )

    async def driver(session):
        return publisher_driver if session is publisher else reader_driver

    @asynccontextmanager
    async def transaction(*args, **kwargs):
        yield reader

    monkeypatch.setattr(capture, "_driver", driver)
    monkeypatch.setattr(preparation, "registry_company_approval_transaction", transaction)
    service = SimpleNamespace(scope_service=SimpleNamespace(reader_sessions=object()))
    for schema, owner in (("control_a", "office_owner"), ("control_b", "office_owner"), ("control_a", "other_owner")):
        await preparation._reserve_capacity(
            service, publisher, SimpleNamespace(control_schema=schema, owner_role=owner), selector()
        )
    keys = [call.args[1] for call in publisher_driver.fetchval.await_args_list]
    assert keys[0] == keys[1] and keys[0] != keys[2]
